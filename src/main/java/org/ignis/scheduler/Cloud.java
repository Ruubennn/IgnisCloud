package org.ignis.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ignis.scheduler.model.*;
import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.ssm.SsmClient;

public class Cloud implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private final TerraformManager terraformManager;
    private final AwsFactory awsFactory;
    private final EC2Operations ec2;
    private final S3Operations s3;
    private final UserDataBuilder userDataBuilder;
    private final BundleCreator bundleCreator;
    private final PayloadResolver payloadResolver;

    private final Map<String, JobMeta> jobs = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, IContainerInfo.IStatus> runtimeStatus = new ConcurrentHashMap<>();
    private final String dockerBin = System.getenv().getOrDefault("IGNIS_DOCKER_BIN", "/usr/bin/docker");

    private final static Map<String, IContainerInfo.IStatus> CLOUD_STATUS = new HashMap<>() {
        {
            put("pending", IContainerInfo.IStatus.ACCEPTED);
            put("running", IContainerInfo.IStatus.RUNNING);
            put("stopping", IContainerInfo.IStatus.RUNNING);
            put("stopped", IContainerInfo.IStatus.FINISHED);
            put("shutting-down", IContainerInfo.IStatus.DESTROYED);
            put("terminated", IContainerInfo.IStatus.DESTROYED);
            put("not_found", IContainerInfo.IStatus.ERROR);
        }
    };

    public Cloud(String url) throws ISchedulerException, Exception {
        LOGGER.info("Initializing Cloud scheduler at: {}", url);

        this.awsFactory = new AwsFactory(resolveRegion());

        // Build clients
        Ec2Client ec2Client = awsFactory.createEc2Client();
        S3Client s3Client = awsFactory.createS3Client();
        SsmClient ssmClient = awsFactory.createSsmClient();

        this.ec2 = new EC2Operations(ec2Client, ssmClient, awsFactory);
        this.terraformManager = new TerraformManager(awsFactory.getRegion().id(), ec2.resolveAvailabilityZone());
        this.s3 = new S3Operations(s3Client);
        this.userDataBuilder = new UserDataBuilder();
        this.bundleCreator = new BundleCreator();
        this.payloadResolver = new PayloadResolver();
    }

    private Region resolveRegion() throws  ISchedulerException {
        String configuredRegion = System.getenv("IGNIS_AWS_REGION");

        if(configuredRegion != null && !configuredRegion.isBlank()) {
            try {
                return Region.of(configuredRegion.trim());
            } catch (Exception e) {
                throw new ISchedulerException("Invalid AWS region '" + configuredRegion + "'. Example: eu-west-1", e);
            }
        }
        try{
            Region auto = new DefaultAwsRegionProviderChain().getRegion();
            if(auto != null) return auto;
        } catch (Exception ignored){}

        throw new ISchedulerException("AWS region not configured. Set IGNIS_AWS_REGION or configure it in ~/.aws/config (aws configure) or export AWS_REGION/AWS_DEFAULT_REGION.");
    }

    private JobMeta resolveJobMeta(String jobId) {
        JobMeta meta = jobs.get(jobId);
        if (meta != null) return meta;

        String bucket = null;
        try {
            bucket = terraformManager.requireOutput("jobs_bucket_name");
        } catch (Exception e) {
            bucket = System.getenv("IGNIS_JOBS_BUCKET");
        }

        if (bucket == null){
            LOGGER.warn("Could not resolve bucket for job {}", jobId);
            return null;
        }

        meta = s3.loadJobMetaFromS3(jobId, bucket);
        if(meta != null){
            jobs.put(jobId, meta);
        }

        return meta;
    }

    private String launchExecutor(String job, int index, IClusterRequest request) throws ISchedulerException {
        String containerName = job + "-executor-" + index;

        // Lanzar contenedor
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add(dockerBin);
            cmd.add("run");
            cmd.add("-d");
            cmd.add("--network"); cmd.add("host");
            cmd.add("--name"); cmd.add(containerName);

            for (var entry : request.resources().env().entrySet()) {
                String value = entry.getValue();
                if (value != null) value = value.trim();
                cmd.add("-e"); cmd.add(entry.getKey() + "=" + value);
            }

            cmd.add("-e"); cmd.add("IGNIS_SCHEDULER_ENV_JOB=" + job);
            cmd.add("-e"); cmd.add("IGNIS_SCHEDULER_ENV_CONTAINER=" + containerName);
            cmd.add("-e"); cmd.add("IGNIS_JOB_ID=" + job);
            cmd.add("-e"); cmd.add("IGNIS_JOB_CONTAINER_DIR=/opt/ignis/jobs");
            cmd.add("-e"); cmd.add("IGNIS_JOB_DIR=/opt/ignis/jobs/" + job);
            cmd.add("-v"); cmd.add("/ignis/dfs:/ignis/dfs");
            cmd.add("-v"); cmd.add("/var/run/docker.sock:/var/run/docker.sock");
            cmd.add("-v"); cmd.add("/opt/ignis/jobs/" + job + ":/opt/ignis/jobs/" + job);
            cmd.add(request.resources().image());

            List<String> executorArgs = new ArrayList<>();
            executorArgs.add("ignis-logger");
            if (request.resources().args() != null && !request.resources().args().isEmpty()) {
                executorArgs.addAll(request.resources().args());
            } else {
                executorArgs.add("ignis-run");
            }
            cmd.addAll(executorArgs);

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            int rc = p.waitFor();
            if (rc != 0) {
                String out = new String(p.getInputStream().readAllBytes());
                throw new ISchedulerException("docker run failed for executor " + index + ": " + out);
            }
        } catch (ISchedulerException e) {
            throw e;
        } catch (Exception e) {
            throw new ISchedulerException("Error launching executor container " + index, e);
        }

        // Configurar authorized_keys
        String publicKey = request.resources().env().get("IGNIS_CRYPTO_PUBLIC");
        if (publicKey != null) {
            try {
                publicKey = publicKey.trim();
                ProcessBuilder mkdirPb = new ProcessBuilder(dockerBin, "exec", containerName,
                        "bash", "-c",
                        "mkdir -p /root/.ssh && chmod 700 /root/.ssh && echo '" + publicKey +
                                "' > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys");
                mkdirPb.redirectErrorStream(true);
                Process mkdirP = mkdirPb.start();
                int rc = mkdirP.waitFor();
                if (rc != 0) {
                    String out = new String(mkdirP.getInputStream().readAllBytes());
                    new ProcessBuilder(dockerBin, "stop", containerName).start().waitFor();
                    throw new ISchedulerException("Failed to configure SSH keys for executor " + index + ": " + out);
                }
            } catch (ISchedulerException e) {
                throw e;
            } catch (Exception e) {
                throw new ISchedulerException("Error configuring SSH keys for executor " + index, e);
            }
        } else {
            LOGGER.warn("IGNIS_CRYPTO_PUBLIC not found, executor {} may fail SSH authentication", containerName);
        }

        // Esperar arranque
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Interrupted while waiting for executor " + index + " to start", e);
        }

        return containerName;
    }

    private IContainerInfo buildExecutorContainerInfo(String containerName, String job, IClusterRequest request) {
        return IContainerInfo.builder()
                .id(containerName)
                .node("localhost")
                .image(request.resources().image())
                .args(request.resources().args() != null ? request.resources().args() : List.of())
                .cpus(request.resources().cpus())
                .gpu(request.resources().gpu())
                .memory(request.resources().memory())
                .writable(true)
                .tmpdir(true)
                .ports(request.resources().ports())
                .binds(List.of())
                .nodelist(List.of())
                .hostnames(Map.of())
                .env(Map.of(
                        "IGNIS_SCHEDULER_ENV_JOB", job,
                        "IGNIS_SCHEDULER_ENV_CONTAINER", containerName
                ))
                .network(IContainerInfo.INetworkMode.BRIDGE)
                .status(IContainerInfo.IStatus.RUNNING)
                .provider(IContainerInfo.IProvider.DOCKER)
                .schedulerOptArgs(Map.of())
                .build();
    }

    private IContainerInfo.IStatus statusFromS3(JobMeta meta) {
        try {
            String key = "jobs/" + meta.jobId() + "/status.json";
            String json = s3.getString(meta.bucket(), key);
            if (json == null || json.isBlank()) return null;

            var node = mapper.readTree(json);
            String state = node.path("state").asText(null);
            if (state == null) return null;

            return switch (state) {
                case "FINISHED" -> IContainerInfo.IStatus.FINISHED;
                case "FAILED" -> IContainerInfo.IStatus.ERROR;
                case "DESTROYED" -> IContainerInfo.IStatus.DESTROYED;
                default -> IContainerInfo.IStatus.UNKNOWN;
            };
        } catch (Exception e) {
            LOGGER.debug("Could not read/parse status.json for job {}", meta.jobId(), e);
            return null;
        }
    }

    private void cleanupInfrastructure(String bucket) {
        try {
            s3.emptyBucket(bucket);
        } catch (Exception e) {
            LOGGER.warn("Failed to empty bucket {}", bucket, e);
        }
        closeClients();
        try {
            terraformManager.destroy();
        } catch (Exception e) {
            LOGGER.warn("Failed to destroy Terraform infrastructure", e);
        }
    }

    private void closeClients() {
        ec2.close();
        s3.close();
    }

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {

        terraformManager.ensureInfrastructure();

        String jobId = ISchedulerUtils.genId().substring(0, 8);
        String finalJobName = name.replace("/", "-") + "-" + jobId;
        LOGGER.info("Creating job with name {} and id {}", finalJobName, jobId);

        String subnet = terraformManager.requireOutput("subnet_id");
        String sg = terraformManager.requireOutput("sg_id");
        String iamRoleArn=""; // TODO: comprobar si con la cuenta AWS Academy puedo usar roles IAM: Antes tenía esto: //String iamRoleArn = terraformManager.requireOutput("iam_role_arn"); & //String iamInstanceProfile = terraformManager.requireOutput("aws_iam_instance_profile");
        String bucket = terraformManager.requireOutput("jobs_bucket_name");

        String iamInstanceProfile = System.getenv("IGNIS_IAM_INSTANCE_PROFILE"); //TODO: estoy usando por defecto: -p ignis.submitter.env.IGNIS_IAM_INSTANCE_PROFILE=EMR_EC2_DefaultRole
        if (iamInstanceProfile == null || iamInstanceProfile.isBlank()) {
            throw new ISchedulerException("Missing IGNIS_IAM_INSTANCE_PROFILE (IAM creation disabled in this AWS account)");
        }
        if(subnet == null || sg == null || bucket == null) { // TODO: IAM /*iamRoleArn == null ||*/
            throw new ISchedulerException("Terraform outputs not found");
        }

        // Prepare Payload
        String bundleKey;
        try {
            List<IBindMount> binds = new ArrayList<>(payloadResolver.buildPayloadBindsFromArgs(driver));
            BundleResult result = bundleCreator.createBundleTarGzHybrid(binds, bucket, jobId, s3);
            bundleKey = s3.uploadJobBundle(bucket, jobId, result.tarGz());
        } catch (Exception e) {
            throw new ISchedulerException("Failed to prepare job payload for job " + jobId, e);
        }

        // Launch EC2 instance
        String instanceId, cmd;
        try {
            String image = driver.resources().image();
            cmd = payloadResolver.resolveCommand(driver);
            String userData = userDataBuilder.buildUserData(awsFactory.getRegion().id(), finalJobName, jobId, bucket, bundleKey, image, cmd);
            InstanceType instanceType = ec2.resolveInstanceType(driver);
            instanceId = ec2.createEC2Instance(finalJobName + "-driver", userData, ec2.resolveAMI(), subnet, sg, iamRoleArn, instanceType, iamInstanceProfile);
        } catch (Exception e) {
            throw new ISchedulerException("Failed to launch EC2 instance for job " + jobId, e);
        }

        // Save metadata
        JobMeta meta = new JobMeta(jobId, finalJobName, bucket, instanceId,
                driver.resources().image(), cmd,
                driver.resources().cpus(), driver.resources().memory(),
                driver.resources().gpu(), driver.resources().args());
        jobs.put(jobId, meta);
        try {
            s3.saveJobMetaToS3(meta);
        } catch (Exception e) {
            LOGGER.warn("Failed to save job meta to S3 for job {}, continuing", jobId, e);
        }

        // Wait for results and download
        // TODO: esta es la espera activa (si no convence puede obviarse, pero me permite descargarle al usuario los ficheros)
        long maxWaitMs = 10 * 60 * 1000; // 10 mins
        long start = System.currentTimeMillis();

        System.out.println("[ignis-cloud] Job running...");
        while (true) {
            IContainerInfo.IStatus status = statusFromS3(meta);

            if (status == IContainerInfo.IStatus.FINISHED) {
                System.out.println("[ignis-cloud] Job completed. Downloading results...");
                try{
                    s3.downloadJob(jobId, bucket);
                    System.out.println("[ignis-cloud] Results downloaded successfully.");
                } catch (Exception e) {
                    LOGGER.warn("Failed to download results for job {}", jobId, e);
                    System.out.println("[ignis-cloud] Warning: could not download results. Available at: s3://" + bucket + "/jobs/" + jobId + "/results/");
                }
                System.out.println("[ignis-cloud] Cleaning up infrastructure...");
                cleanupInfrastructure(bucket);
                System.out.println("[ignis-cloud] Infrastructure cleaned up.");
                break;

            } else if (status == IContainerInfo.IStatus.ERROR || status == IContainerInfo.IStatus.DESTROYED) {
                System.out.println("\n[ignis-cloud] Job failed with status: " + status);
                LOGGER.error("Job {} failed with status {}", jobId, status);
                cleanupInfrastructure(bucket);
                break;
            }
            // TIMEOUT CHECK
            if (System.currentTimeMillis() - start > maxWaitMs) {
                System.out.println("\n[ignis-cloud] Timeout reached. Results at: s3://" + bucket + "/jobs/" + jobId + "/");
                cleanupInfrastructure(bucket);
                break;
            }
            try{
                Thread.sleep(1000);
            } catch(Exception e){
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while waiting for job {}", jobId);
                break;
            }

        }
        LOGGER.info("Created job with name {} and id {}", finalJobName, jobId);
        return jobId;
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        LOGGER.info("Canceling job with id {}", id);

        JobMeta meta = jobs.get(id);
        if (meta == null) {
            meta = resolveJobMeta(id);
        }
        if (meta == null) {
            throw new ISchedulerException("job " + id + " not found");
        }
        try{
            String key = "jobs/" + id + "/status.json";
            String body = "{\"state\":\"DESTROYED\",\"rc\":143}";
            String type = "application/json";
            s3.putString(meta.bucket(), key, body,  type);
        } catch (Exception e){
            LOGGER.warn("Failed to update status.json for job {}, continuating with termination", id, e);
        }

        try{
            String instanceId = meta.instanceId();
            if(instanceId != null && !instanceId.isBlank()) {
                    ec2.terminateInstance(instanceId);
                    LOGGER.info("EC2 instance {} terminated for job {}", instanceId, id);
            }
            jobs.remove(id);
        }catch(Exception e){
            throw new ISchedulerException("Error terminating EC2 instance for job " + id, e);
        }
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        LOGGER.info("Getting job with id {}", id);

        JobMeta meta = jobs.get(id);
        if (meta == null) {
            meta = resolveJobMeta(id);
            if(meta != null){
                jobs.put(id, meta);
            }
        }
        if (meta == null) {
            throw new ISchedulerException("job " + id + " not found");
        }

        try{
            IContainerInfo.IStatus status = statusFromS3(meta);
            if (status == null) {
                status = runtimeStatus.getOrDefault(id, IContainerInfo.IStatus.ACCEPTED);
                if(status == IContainerInfo.IStatus.ACCEPTED){
                    runtimeStatus.put(id, IContainerInfo.IStatus.RUNNING);
                }
            } else {
                runtimeStatus.remove(id);
            }

            IContainerInfo container = IContainerInfo.builder()
                    .id(meta.instanceId())
                    .node("localhost")
                    .image(meta.image())
                    .args(meta.args() != null ? meta.args() : List.of())
                    .cpus(meta.cpus())
                    .gpu(meta.gpu())
                    .memory(meta.memory())
                    .writable(true)
                    .tmpdir(true)
                    .ports(List.of())
                    .binds(List.of())
                    .nodelist(List.of())
                    .hostnames(Map.of())
                    .env(Map.of(
                            "IGNIS_SCHEDULER_ENV_JOB", meta.jobId(),
                            "IGNIS_SCHEDULER_ENV_CONTAINER", meta.instanceId()
                    ))
                    .network(IContainerInfo.INetworkMode.BRIDGE)
                    .status(status)
                    .provider(IContainerInfo.IProvider.DOCKER)
                    .schedulerOptArgs(Map.of())
                    .build();

            IClusterInfo cluster = IClusterInfo.builder()
                    .id("0-driver")
                    .instances(1)
                    .containers(List.of(container))
                    .build();
            return IJobInfo.builder()
                    .name(meta.jobName())
                    .id(meta.jobId())
                    .clusters(List.of(cluster))
                    .build();
        } catch (Exception e) {
                throw new ISchedulerException("Error getting job " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        LOGGER.info("createCluster job {} instances={}", job, request.instances());

        int instances = request.instances();
        var containerIds = new ArrayList<String>();

        // Lanzar executors
        for (int i = 0; i < instances; i++) {
            String containerName = launchExecutor(job, i, request);
            containerIds.add(containerName);
        }

        // Esperar a que el executor esté listo en el puerto 1963
        int maxWait = 30;
        boolean ready = false;
        for (int attempt = 0; attempt < maxWait; attempt++) {
            try (var socket = new Socket("localhost", 1963)) {
                ready = true;
                break;
            } catch (Exception e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new ISchedulerException("Interrupted while waiting for executor port", e1);
                }
            }
        }

        if (!ready) {
            throw new ISchedulerException("Executor never became ready on port 1963 after " + maxWait + " seconds");
        }

        // Construir lista de containers
        var containers = new ArrayList<IContainerInfo>();
        for (int i = 0; i < containerIds.size(); i++) {
            containers.add(buildExecutorContainerInfo(containerIds.get(i), job, request));
        }

        return IClusterInfo.builder()
                .id(request.name())
                .instances(instances)
                .containers(containers)
                .build();
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
        LOGGER.info("Destroying cluster {} for job {}", id, job);
        try{ // List executor containers
            ProcessBuilder pb = new ProcessBuilder(dockerBin, "ps", "-q", "--filter", "name=" + job + "-executor");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            String output = new String(p.getInputStream().readAllBytes()).trim();

            if(output.isEmpty()){
                LOGGER.info("No executor containers found for job {}", job);
                return;
            }

            for(String containerId: output.split("\n")){
                if(containerId.isBlank()) continue;
                try{
                    new ProcessBuilder(dockerBin, "stop", containerId.trim()).start().waitFor();
                    LOGGER.info("Executor container {} stopped", containerId);
                }catch(Exception e){
                    LOGGER.warn("Failed to stop executor container {}: {}", containerId.trim(), e.getMessage());
                }
            }

        } catch (Exception e) {
            throw new ISchedulerException("Failed to destroy cluster " + id + " for job " + job, e);
        }
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        LOGGER.info("Getting cluster {} for job {}", id, job);

        JobMeta meta = resolveJobMeta(job);
        if(meta == null){
            LOGGER.warn("No job metadata found for job {}", job);
        }

        try{
            ProcessBuilder pb = new  ProcessBuilder(dockerBin, "ps", "-a", "--filter", "name=" + job + "-executor", "--format", "{{.Names}}");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            String output = new String(p.getInputStream().readAllBytes()).trim();
            p.waitFor();

            var containers  = new ArrayList<IContainerInfo>();

            if (!output.isEmpty()) {
                for (String containerName : output.split("\n")) {
                    if (containerName.isBlank()) continue;
                    containerName = containerName.trim();

                    IContainerInfo.IStatus status = getContainerStatus(job, containerName);

                    var builder = IContainerInfo.builder()
                            .id(containerName)
                            .node("localhost")
                            .writable(true)
                            .tmpdir(true)
                            .ports(List.of())
                            .binds(List.of())
                            .nodelist(List.of())
                            .hostnames(Map.of())
                            .env(Map.of(
                                    "IGNIS_SCHEDULER_ENV_JOB", job,
                                    "IGNIS_SCHEDULER_ENV_CONTAINER", containerName
                            ))
                            .network(IContainerInfo.INetworkMode.BRIDGE)
                            .status(status)
                            .provider(IContainerInfo.IProvider.DOCKER)
                            .schedulerOptArgs(Map.of());

                    if (meta != null) {
                        builder.image(meta.image())
                                .cpus(meta.cpus())
                                .memory(meta.memory())
                                .gpu(meta.gpu())
                                .args(meta.args() != null ? meta.args() : List.of());
                    } else {
                        builder.image("")
                                .cpus(1)
                                .memory(0L)
                                .gpu(null)
                                .args(List.of());
                    }

                    containers.add(builder.build());
                }
            }

            if (containers.isEmpty()) {
                LOGGER.warn("No executor containers found for cluster {} job {}", id, job);
            }
            return IClusterInfo.builder()
                    .id(id)
                    .instances(containers.size())
                    .containers(containers)
                    .build();

        }catch (Exception e) {
            throw new ISchedulerException("Failed to get cluster " + id + " for job " + job, e);
        }
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException {
        LOGGER.info("Repairing cluster {} for job {}", cluster.id(), job);

        var newContainers = new ArrayList<IContainerInfo>(cluster.containers());
        boolean repaired = false;

        for (int i = 0; i < cluster.containers().size(); i++) {
            IContainerInfo container = cluster.containers().get(i);
            IContainerInfo.IStatus status = getContainerStatus(job, container.id());

            if (status == IContainerInfo.IStatus.RUNNING) {
                LOGGER.debug("Container {} is healthy, skipping", container.id());
                continue;
            }

            LOGGER.warn("Container {} is not running (status: {}), attempting repair", container.id(), status);

            // Eliminar contenedor si existe
            try {
                new ProcessBuilder(dockerBin, "rm", "-f", container.id())
                        .start().waitFor();
            } catch (Exception e) {
                LOGGER.warn("Failed to remove container {}: {}", container.id(), e.getMessage());
            }

            // Relanzar y actualizar
            String containerName = launchExecutor(job, i, request);
            newContainers.set(i, buildExecutorContainerInfo(containerName, job, request));

            repaired = true;
            LOGGER.info("Successfully repaired executor container {}", containerName);
        }

        if (!repaired) {
            LOGGER.info("No containers needed repair for cluster {} job {}", cluster.id(), job);
            return cluster;
        }

        return IClusterInfo.builder()
                .id(cluster.id())
                .instances(cluster.instances())
                .containers(newContainers)
                .build();
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        if(id == null || id.isBlank()) {
            throw new ISchedulerException("Container id cannot be null or empty");
        }
        // Docker container (EC2 executor)
        if(!id.startsWith("i-")) {
            try{
                ProcessBuilder pb = new ProcessBuilder(dockerBin, "inspect", "--format", "{{.State.Status}}", id);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                String output = new String(p.getInputStream().readAllBytes()).trim();
                p.waitFor();

                LOGGER.debug("Container {} status: {}", id, output);
                return switch (output) {
                    case "created", "restarting" -> IContainerInfo.IStatus.ACCEPTED;
                    case "running" -> IContainerInfo.IStatus.RUNNING;
                    case "exited"  -> IContainerInfo.IStatus.FINISHED;
                    case "dead"    -> IContainerInfo.IStatus.ERROR;
                    default -> IContainerInfo.IStatus.UNKNOWN;
                };
            } catch(Exception e){
                LOGGER.warn("Failed to get status for container {}: {}", id, e.getMessage());
                return IContainerInfo.IStatus.UNKNOWN;
            }
        }

        // EC2 instance
        try{
            String awsState = ec2.getInstanceState(id);
            if(awsState == null){
                LOGGER.warn("Could not get EC2 state for instance {}", id);
                return IContainerInfo.IStatus.UNKNOWN;
            }
            LOGGER.debug("EC2 state for instance {}: {}", id, awsState);
            return CLOUD_STATUS.getOrDefault(awsState.toLowerCase(), IContainerInfo.IStatus.UNKNOWN);
        } catch (Exception e) {
            throw new ISchedulerException("Failed to get EC2 instance status for " + id, e);
        }
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        LOGGER.info("Performing health check for Cloud scheduler");

        // Check if Docker is available
        boolean isRuntime = Boolean.parseBoolean(System.getenv("IGNIS_CLOUD_RUNTIME"));
        if(isRuntime){
            try{
                ProcessBuilder pb = new ProcessBuilder(dockerBin, "info");
                pb.redirectErrorStream(true);
                Process p = pb.start();
                int rc = p.waitFor();
                if (rc != 0) {
                    throw new ISchedulerException("Docker is not available: " + new String(p.getInputStream().readAllBytes()));
                }
                LOGGER.debug("Docker health check passed");
            } catch(ISchedulerException e){
                throw e;
            } catch(Exception e){
                throw new ISchedulerException("Failed to check Docker availability", e);
            }
        }

        // Verify AWS conectivity
        try{
            ec2.getInstanceState("i-0000000000000");
            LOGGER.info("AWS health check passed");
        } catch(ISchedulerException e){
            LOGGER.info("AWS health check passed");
        } catch(Exception e){
            throw new ISchedulerException("Failed to check AWS health check", e);
        }
        LOGGER.info("Finished health check for Cloud scheduler");
    }
}