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

    private static final class PendingExecutor {
        final String containerName;
        final String instanceId;

        PendingExecutor(String containerName, String instanceId) {
            this.containerName = containerName;
            this.instanceId = instanceId;
        }
    }

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

    /*private IContainerInfo buildExecutorContainerInfo(String containerName, String job, IClusterRequest request) {
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
    }*/

    /*private IContainerInfo buildExecutorContainerInfo(String instanceId, String containerName, String privateIp, String job, IClusterRequest request) {

        Map<String, String> env = new HashMap<>(request.resources().env());
        env.put("IGNIS_SCHEDULER_ENV_JOB", job);
        env.put("IGNIS_SCHEDULER_ENV_CONTAINER", containerName);

        return IContainerInfo.builder()
                .id(instanceId)
                .node(privateIp)
                .image(request.resources().image())
                .args(request.resources().args() != null ? request.resources().args() : List.of())
                .cpus(request.resources().cpus())
                .gpu(request.resources().gpu())
                .memory(request.resources().memory())
                .writable(true)
                .tmpdir(true)
                .ports(List.of(new IPortMapping(1963, 1963, IPortMapping.Protocol.TCP)))
                .binds(List.of())
                .nodelist(List.of())
                .hostnames(Map.of(containerName, privateIp))
                .env(env)
                .network(IContainerInfo.INetworkMode.HOST)
                .status(IContainerInfo.IStatus.RUNNING)
                .provider(IContainerInfo.IProvider.DOCKER)
                .schedulerOptArgs(Map.of())
                .build();
    }*/

    private IContainerInfo buildExecutorContainerInfo(
            String instanceId,
            String containerName,
            ReadyInfo ready,
            String job,
            IClusterRequest request
    ) {
        Map<String, String> env = new HashMap<>(request.resources().env());
        env.put("IGNIS_SCHEDULER_ENV_JOB", job);
        env.put("IGNIS_SCHEDULER_ENV_CONTAINER", containerName);

        return IContainerInfo.builder()
                .id(instanceId)
                .node(ready.ip)
                .image(request.resources().image())
                .args(request.resources().args() != null ? request.resources().args() : List.of())
                .cpus(request.resources().cpus())
                .gpu(request.resources().gpu())
                .memory(request.resources().memory())
                .writable(true)
                .tmpdir(true)
                .ports(List.of(new IPortMapping(ready.port, ready.port, IPortMapping.Protocol.TCP)))
                .binds(List.of())
                .nodelist(List.of())
                .hostnames(Map.of(containerName, ready.ip))
                .env(env)
                .network(IContainerInfo.INetworkMode.HOST)
                .status(IContainerInfo.IStatus.RUNNING)
                .provider(IContainerInfo.IProvider.DOCKER)
                .schedulerOptArgs(Map.of())
                .build();
    }

    private IContainerInfo buildExecutorContainerInfoFromMeta(
            String instanceId,
            String containerName,
            ReadyInfo ready,
            String job,
            JobMeta meta
    ) {
        Map<String, String> env = new HashMap<>();
        env.put("IGNIS_SCHEDULER_ENV_JOB", job);
        env.put("IGNIS_SCHEDULER_ENV_CONTAINER", containerName);

        return IContainerInfo.builder()
                .id(instanceId)
                .node(ready.ip)
                .image(meta.image())
                .args(meta.args() != null ? meta.args() : List.of())
                .cpus(meta.cpus())
                .gpu(meta.gpu())
                .memory(meta.memory())
                .writable(true)
                .tmpdir(true)
                .ports(List.of(new IPortMapping(ready.port, ready.port, IPortMapping.Protocol.TCP)))
                .binds(List.of())
                .nodelist(List.of())
                .hostnames(Map.of(containerName, ready.ip))
                .env(env)
                .network(IContainerInfo.INetworkMode.HOST)
                .status(IContainerInfo.IStatus.RUNNING)
                .provider(IContainerInfo.IProvider.DOCKER)
                .schedulerOptArgs(Map.of())
                .build();
    }

    private ReadyInfo loadExecutorReadyIfPresent(JobMeta meta, String containerName) throws ISchedulerException {
        if (meta == null) return null;

        String key = "jobs/" + meta.jobId() + "/ready/" + containerName + ".json";
        String failedKey = "jobs/" + meta.jobId() + "/ready/" + containerName + ".failed.json";

        String failedJson = s3.getString(meta.bucket(), failedKey);
        if (failedJson != null && !failedJson.isBlank()) {
            throw new ISchedulerException("Executor " + containerName + " reported failure: " + failedJson);
        }

        String json = s3.getString(meta.bucket(), key);
        if (json == null || json.isBlank()) {
            return null;
        }

        try {
            ReadyInfo ready = mapper.readValue(json, ReadyInfo.class);
            if (ready != null && ready.isValid()) {
                return ready;
            }
            LOGGER.warn("Executor {} ready file exists but is invalid: {}", containerName, json);
            return null;
        } catch (Exception e) {
            throw new ISchedulerException("Failed to parse ready file for executor " + containerName, e);
        }
    }

    private List<String> normalizeExecutorArgs(List<String> args) {
        List<String> executorArgs = new ArrayList<>(args != null ? args : List.of());

        if (executorArgs.size() >= 3
                && "ignis-sshserver".equals(executorArgs.get(0))
                && "executor".equals(executorArgs.get(1))) {
            executorArgs.set(2, "0");
        }

        return executorArgs;
    }

    private int resolvePrelaunchExecutors(IClusterRequest... executors) throws ISchedulerException {
        if (executors != null && executors.length > 0) {
            return Arrays.stream(executors)
                    .filter(Objects::nonNull)
                    .mapToInt(IClusterRequest::instances)
                    .sum();
        }

        String raw = System.getenv("IGNIS_PRELAUNCH_EXECUTORS");
        if (raw == null || raw.isBlank()) {
            return 0;
        }

        try {
            int value = Integer.parseInt(raw.trim());
            return Math.max(value, 0);
        } catch (NumberFormatException e) {
            throw new ISchedulerException("Invalid IGNIS_PRELAUNCH_EXECUTORS='" + raw + "'", e);
        }
    }

    private List<String> prelaunchExecutorArgs() {
        return List.of("ignis-sshserver", "executor", "0");
    }

    private String waitForDriverIp(JobMeta meta) throws ISchedulerException {
        String key = "jobs/" + meta.jobId() + "/driver-ip.txt";
        long deadline = System.currentTimeMillis() + (6 * 60 * 1000);

        while (System.currentTimeMillis() < deadline) {
            String driverIp = s3.getString(meta.bucket(), key);
            if (driverIp != null && !driverIp.isBlank()) {
                return driverIp.trim();
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ISchedulerException("Interrupted while waiting for driver-ip.txt of job " + meta.jobId(), e);
            }
        }

        throw new ISchedulerException("Timeout waiting for s3://" + meta.bucket() + "/" + key);
    }

    private String findInstanceIdByName(List<Instance> instances, String containerName) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }

        for (Instance inst : instances) {
            String name = inst.tags().stream()
                    .filter(t -> "Name".equals(t.key()))
                    .map(Tag::value)
                    .findFirst()
                    .orElse(null);
            if (containerName.equals(name)) {
                return inst.instanceId();
            }
        }
        return null;
    }

    private void prelaunchExecutors(JobMeta meta, IClusterRequest baseRequest, int totalExecutors) throws ISchedulerException {
        if (totalExecutors <= 0) {
            return;
        }

        String region = awsFactory.getRegion().id();
        String subnet = System.getenv("IGNIS_SUBNET_ID");
        String sg = System.getenv("IGNIS_SG_ID");
        String ami = System.getenv("IGNIS_AMI");
        String instanceTypeStr = System.getenv("IGNIS_INSTANCE_TYPE");

        InstanceType instanceType = null;
        if (instanceTypeStr != null && !instanceTypeStr.isBlank()) {
            String parsed = instanceTypeStr.toLowerCase().replaceAll("_", ".");
            instanceType = InstanceType.fromValue(parsed);
        }

        if (subnet == null || sg == null || ami == null || instanceType == null) {
            subnet = terraformManager.requireOutput("subnet_id");
            sg = terraformManager.requireOutput("sg_id");
            ami = ec2.resolveAMI();
            instanceType = ec2.resolveInstanceType(baseRequest);
        }

        Map<String, String> executorEnv = new HashMap<>();
        if (baseRequest != null && baseRequest.resources() != null && baseRequest.resources().env() != null) {
            executorEnv.putAll(baseRequest.resources().env());
        }

        List<Instance> existing = ec2.describeInstancesByTag(meta.jobId());
        var pending = new ArrayList<PendingExecutor>();

        for (int i = 0; i < totalExecutors; i++) {
            String containerName = meta.jobId() + "-executor-" + i;
            String existingId = findInstanceIdByName(existing, containerName);
            if (existingId != null) {
                System.out.println("[ignis-cloud] PRELAUNCH REUSE executor " + containerName + " instanceId=" + existingId);
                pending.add(new PendingExecutor(containerName, existingId));
                continue;
            }

            String userData = userDataBuilder.buildExecutorUserData(
                    region,
                    meta.jobId(),
                    containerName,
                    meta.bucket(),
                    meta.bundleKey(),
                    meta.image(),
                    executorEnv,
                    prelaunchExecutorArgs()
            );

            String instanceId = ec2.createEC2Instance(
                    containerName,
                    userData,
                    ami,
                    subnet,
                    sg,
                    "",
                    instanceType,
                    "",
                    meta.jobId()
            );

            System.out.println("[ignis-cloud] PRELAUNCH REQUEST executor " + containerName + " instanceId=" + instanceId);
            pending.add(new PendingExecutor(containerName, instanceId));
        }

        for (PendingExecutor p : pending) {
            ec2.waitUntilRunning(p.instanceId);
            String privateIp = ec2.waitForPrivateIp(p.instanceId);
            System.out.println("[ignis-cloud] PRELAUNCH RUNNING executor " + p.containerName + " ec2Ip=" + privateIp);
            ReadyInfo ready = waitForExecutorReady(meta, p.containerName);
            System.out.println("[ignis-cloud] PRELAUNCH READY executor " + p.containerName + " -> " + ready.ip + ":" + ready.port);
        }
    }

    private ReadyInfo waitForExecutorReady(JobMeta meta, String containerName) throws ISchedulerException {
        String key = "jobs/" + meta.jobId() + "/ready/" + containerName + ".json";
        String failedKey = "jobs/" + meta.jobId() + "/ready/" + containerName + ".failed.json";

        long deadline = System.currentTimeMillis() + (6 * 60 * 1000); // 6 min
        Exception lastError = null;

        while (System.currentTimeMillis() < deadline) {
            try {
                String failedJson = s3.getString(meta.bucket(), failedKey);
                if (failedJson != null && !failedJson.isBlank()) {
                    throw new ISchedulerException("Executor " + containerName + " reported failure: " + failedJson);
                }

                String json = s3.getString(meta.bucket(), key);
                if (json != null && !json.isBlank()) {
                    ReadyInfo ready = mapper.readValue(json, ReadyInfo.class);
                    if (ready != null && ready.isValid()) {
                        LOGGER.info("Executor {} ready from S3: {}", containerName, ready);
                        return ready;
                    }
                    LOGGER.warn("Executor {} ready file exists but is invalid: {}", containerName, json);
                }
            } catch (ISchedulerException e) {
                throw e;
            } catch (Exception e) {
                lastError = e;
                LOGGER.debug("Waiting for ready file of {} at s3://{}/{}", containerName, meta.bucket(), key, e);
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ISchedulerException("Interrupted while waiting for ready file of " + containerName, e);
            }
        }

        throw new ISchedulerException(
                "Timeout waiting for executor ready file: s3://" + meta.bucket() + "/" + key,
                lastError
        );
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
        String iamRoleArn = "";
        String bucket = terraformManager.requireOutput("jobs_bucket_name");

        String iamInstanceProfile = System.getenv("IGNIS_IAM_INSTANCE_PROFILE");
        if (iamInstanceProfile == null || iamInstanceProfile.isBlank()) {
            throw new ISchedulerException("Missing IGNIS_IAM_INSTANCE_PROFILE (IAM creation disabled in this AWS account)");
        }
        if (subnet == null || sg == null || bucket == null) {
            throw new ISchedulerException("Terraform outputs not found");
        }

        String bundleKey;
        try {
            List<IBindMount> binds = new ArrayList<>(payloadResolver.buildPayloadBindsFromArgs(driver));
            BundleResult result = bundleCreator.createBundleTarGzHybrid(binds, bucket, jobId, s3);
            bundleKey = s3.uploadJobBundle(bucket, jobId, result.tarGz());
        } catch (Exception e) {
            throw new ISchedulerException("Failed to prepare job payload for job " + jobId, e);
        }

        String instanceId;
        String cmd;
        try {
            String image = driver.resources().image();
            cmd = payloadResolver.resolveCommand(driver);
            String ami = ec2.resolveAMI();
            InstanceType instanceType = ec2.resolveInstanceType(driver);

            String userData = userDataBuilder.buildUserData(
                    awsFactory.getRegion().id(),
                    finalJobName,
                    jobId,
                    bucket,
                    bundleKey,
                    image,
                    cmd,
                    subnet,
                    sg,
                    instanceType.name(),
                    ami
            );
            instanceId = ec2.createEC2Instance(
                    finalJobName + "-driver",
                    userData,
                    ami,
                    subnet,
                    sg,
                    iamRoleArn,
                    instanceType,
                    iamInstanceProfile
            );
        } catch (Exception e) {
            throw new ISchedulerException("Failed to launch EC2 instance for job " + jobId, e);
        }

        JobMeta meta = new JobMeta(
                jobId,
                finalJobName,
                bucket,
                instanceId,
                bundleKey,
                driver.resources().image(),
                cmd,
                driver.resources().cpus(),
                driver.resources().memory(),
                driver.resources().gpu(),
                driver.resources().args()
        );
        jobs.put(jobId, meta);
        try {
            s3.saveJobMetaToS3(meta);
        } catch (Exception e) {
            LOGGER.warn("Failed to save job meta to S3 for job {}, continuing", jobId, e);
        }

        int prelaunchExecutors = resolvePrelaunchExecutors(executors);
        if (prelaunchExecutors > 0) {
            System.out.println("[ignis-cloud] PRELAUNCH enabled with " + prelaunchExecutors + " executor(s)");
            String driverIp = waitForDriverIp(meta);
            System.out.println("[ignis-cloud] PRELAUNCH driver IP detected: " + driverIp);

            IClusterRequest prelaunchSpec = (executors != null && executors.length > 0 && executors[0] != null)
                    ? executors[0]
                    : driver;
            prelaunchExecutors(meta, prelaunchSpec, prelaunchExecutors);
        } else {
            System.out.println("[ignis-cloud] PRELAUNCH disabled (set IGNIS_PRELAUNCH_EXECUTORS to enable it)");
        }

        System.out.println("[ignis-cloud] Driver lanzado correctamente. Job ID: " + jobId);
        System.out.println("[ignis-cloud] Esperando a que el framework llame a createCluster() para los executors...");

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
        }catch(Exception e){
            throw new ISchedulerException("Error terminating EC2 instance for job " + id, e);
        } finally {
            jobs.remove(id);
        }

        boolean isRuntime = Boolean.parseBoolean(System.getenv("IGNIS_CLOUD_RUNTIME"));
        if (!isRuntime) {
            //cleanupInfrastructure(meta.bucket());
        } else {
            LOGGER.info("Runtime mode: skipping infrastructure cleanup for job {}", id);
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
        System.out.println("[ignis-cloud] ENTER createCluster job=" + job + " instances=" + request.instances());

        JobMeta meta = resolveJobMeta(job);
        if (meta == null) {
            throw new ISchedulerException("Cannot create cluster: job meta not found for " + job);
        }

        String region = awsFactory.getRegion().id();
        String subnet = System.getenv("IGNIS_SUBNET_ID");
        String sg = System.getenv("IGNIS_SG_ID");
        String ami = System.getenv("IGNIS_AMI");
        String instanceTypeStr = System.getenv("IGNIS_INSTANCE_TYPE");

        InstanceType instanceType = null;
        if (instanceTypeStr != null && !instanceTypeStr.isBlank()) {
            String parsed = instanceTypeStr.toLowerCase().replaceAll("_", ".");
            instanceType = InstanceType.fromValue(parsed);
        }

        if (subnet == null || sg == null || ami == null || instanceType == null) {
            subnet = terraformManager.requireOutput("subnet_id");
            sg = terraformManager.requireOutput("sg_id");
            ami = ec2.resolveAMI();
            instanceType = ec2.resolveInstanceType(request);
        }

        List<Instance> existing = ec2.describeInstancesByTag(job);
        var pending = new ArrayList<PendingExecutor>();
        var containers = new ArrayList<IContainerInfo>();

        for (int i = 0; i < request.instances(); i++) {
            String containerName = job + "-executor-" + i;
            String existingId = findInstanceIdByName(existing, containerName);
            if (existingId != null) {
                System.out.println("[ignis-cloud] REUSE executor " + containerName + " instanceId=" + existingId);
                pending.add(new PendingExecutor(containerName, existingId));
                continue;
            }

            List<String> executorArgs = normalizeExecutorArgs(request.resources().args());
            String userData = userDataBuilder.buildExecutorUserData(
                    region,
                    job,
                    containerName,
                    meta.bucket(),
                    meta.bundleKey(),
                    meta.image(),
                    request.resources().env(),
                    executorArgs
            );

            String instanceId = ec2.createEC2Instance(
                    containerName,
                    userData,
                    ami,
                    subnet,
                    sg,
                    "",
                    instanceType,
                    "",
                    job
            );

            System.out.println("[ignis-cloud] REQUEST executor " + containerName + " instanceId=" + instanceId);
            pending.add(new PendingExecutor(containerName, instanceId));
        }

        for (PendingExecutor p : pending) {
            ec2.waitUntilRunning(p.instanceId);
            String privateIp = ec2.waitForPrivateIp(p.instanceId);
            System.out.println("[ignis-cloud] RUNNING executor " + p.containerName + " ec2Ip=" + privateIp);
            System.out.println("[ignis-cloud] WAIT READY " + p.containerName);
            ReadyInfo ready = waitForExecutorReady(meta, p.containerName);
            System.out.println("[ignis-cloud] READY OK " + p.containerName + " -> " + ready.ip + ":" + ready.port);

            containers.add(buildExecutorContainerInfo(
                    p.instanceId,
                    p.containerName,
                    ready,
                    job,
                    request
            ));
        }

        System.out.println("[ignis-cloud] RETURN createCluster job=" + job);
        return IClusterInfo.builder()
                .id(request.name())
                .instances(request.instances())
                .containers(containers)
                .build();
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
        LOGGER.info("Destroying cluster {} for job {}", id, job);
        ec2.terminateInstancesByTag(job);
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        LOGGER.info("Getting cluster {} for job {}", id, job);

        JobMeta meta = resolveJobMeta(job);

        try {
            List<Instance> instances = ec2.describeInstancesByTag(job);
            var containers = new ArrayList<IContainerInfo>();

            for (var inst : instances) {
                String instanceId = inst.instanceId();
                String privateIp = inst.privateIpAddress() != null ? inst.privateIpAddress() : "";
                String containerName = inst.tags().stream()
                        .filter(t -> "Name".equals(t.key()))
                        .map(Tag::value)
                        .findFirst().orElse(instanceId);

                IContainerInfo.IStatus status = CLOUD_STATUS.getOrDefault(
                        inst.state().nameAsString().toLowerCase(),
                        IContainerInfo.IStatus.UNKNOWN);

                ReadyInfo ready = null;
                if (meta != null && status == IContainerInfo.IStatus.RUNNING) {
                    try {
                        ready = loadExecutorReadyIfPresent(meta, containerName);
                    } catch (Exception e) {
                        LOGGER.warn("Could not resolve ready info for executor {}", containerName, e);
                    }
                }

                if (meta != null && ready != null) {
                    containers.add(buildExecutorContainerInfoFromMeta(
                            instanceId,
                            containerName,
                            ready,
                            job,
                            meta
                    ));
                    continue;
                }

                var builder = IContainerInfo.builder()
                        .id(instanceId)
                        .node(privateIp)
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
                    builder.image("").cpus(1).memory(0L).gpu(null).args(List.of());
                }

                containers.add(builder.build());
            }

            if (containers.isEmpty()) {
                LOGGER.warn("No executor containers found for cluster {} job {}", id, job);
            }

            return IClusterInfo.builder()
                    .id(id)
                    .instances(containers.size())
                    .containers(containers)
                    .build();

        } catch (Exception e) {
            throw new ISchedulerException("Failed to get cluster " + id + " for job " + job, e);
        }
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException {
        LOGGER.info("Repairing cluster {} for job {}", cluster.id(), job);

        JobMeta meta = resolveJobMeta(job);
        if (meta == null) {
            throw new ISchedulerException("Cannot repair cluster: job meta not found for " + job);
        }

        String subnet = System.getenv("IGNIS_SUBNET_ID");
        String sg = System.getenv("IGNIS_SG_ID");
        if (subnet == null || sg == null) {
            subnet = terraformManager.requireOutput("subnet_id");
            sg = terraformManager.requireOutput("sg_id");
        }

        String region = awsFactory.getRegion().id();

        var newContainers = new ArrayList<IContainerInfo>(cluster.containers());
        boolean repaired = false;

        for (int i = 0; i < cluster.containers().size(); i++) {
            IContainerInfo container = cluster.containers().get(i);

            IContainerInfo.IStatus status = getContainerStatus(job, container.id());
            if (status == IContainerInfo.IStatus.RUNNING) {
                LOGGER.debug("Executor {} is healthy, skipping", container.id());
                continue;
            }

            LOGGER.warn("Executor {} is not running (status={}), relaunching", container.id(), status);

            try {
                ec2.terminateInstance(container.id());
            } catch (Exception e) {
                LOGGER.warn("Could not terminate instance {}: {}", container.id(), e.getMessage());
            }

            String containerName = job + "-executor-" + i;
            List<String> executorArgs = normalizeExecutorArgs(request.resources().args());

            String userData = userDataBuilder.buildExecutorUserData(
                    region,
                    job,
                    containerName,
                    meta.bucket(),
                    meta.bundleKey(),
                    meta.image(),
                    request.resources().env(),
                    executorArgs
            );

            InstanceType instanceType = ec2.resolveInstanceType(request);
            String newInstanceId = ec2.createEC2Instance(
                    containerName, userData, ec2.resolveAMI(),
                    subnet, sg, "", instanceType, "", job
            );

            ec2.waitUntilRunning(newInstanceId);
            String privateIp = ec2.waitForPrivateIp(newInstanceId);

            LOGGER.warn("WAITING READY JSON for repaired executor {} job {}", containerName, job);
            ReadyInfo ready = waitForExecutorReady(meta, containerName);
            LOGGER.warn("READY JSON OK for repaired executor {} -> {}:{}", containerName, ready.ip, ready.port);

            LOGGER.info("Repaired executor {}: newInstanceId={} ec2Ip={} readyIp={} readyPort={}",
                    containerName, newInstanceId, privateIp, ready.ip, ready.port);

            newContainers.set(i, buildExecutorContainerInfo(newInstanceId, containerName, ready, job, request));
            repaired = true;
        }

        if (!repaired) {
            LOGGER.info("No executors needed repair for cluster {} job {}", cluster.id(), job);
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

        // Verify AWS conectivity
        try {
            ec2.verifyConnectivity();
            LOGGER.info("AWS health check passed");
        } catch (Exception e) {
            throw new ISchedulerException("AWS connectivity check failed", e);
        }
        LOGGER.info("Finished health check for Cloud scheduler");
    }
}