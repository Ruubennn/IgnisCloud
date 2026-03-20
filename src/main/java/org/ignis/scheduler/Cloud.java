package org.ignis.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ignis.scheduler.model.*;
import java.io.*;
import java.net.Socket;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

public class Cloud implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private final TerraformManager terraformManager;
    private final AwsFactory awsFactory;
    private final EC2Operations ec2;
    private final S3Operations s3;
    private final UserDataBuilder userDataBuilder;
    private final BundleCreator bundleCreator;
    private final PayloadResolver payloadResolver;
    //private final ImagePublisher imagePublisher;

    private static record JobMeta(
      String jobId,
      String jobName,
      String bucket,
      String instanceId,
      String image,
      String cmd,
      int cpus,
      long memory,
      String gpu,
      List<String> args
      // ports, binds, hostnames...
    ){}

    private final Map<String, JobMeta> jobs = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile boolean infrastructureReady = false;

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

    private final Map<String, IContainerInfo.IStatus> runtimeStatus = new ConcurrentHashMap<>();
    private final String dockerBin = System.getenv().getOrDefault("IGNIS_DOCKER_BIN", "/usr/bin/docker");

    public Cloud(String url) throws ISchedulerException, Exception {
        LOGGER.info("Initializing Cloud scheduler at: {}", url);

        this.awsFactory = new AwsFactory(isLocalStackEnvironment(), resolveRegion());
        this.terraformManager = new TerraformManager(awsFactory.getRegion().id());
        this.ec2 = new EC2Operations(awsFactory);
        this.s3 = new S3Operations(awsFactory);
        this.userDataBuilder = new UserDataBuilder();
        this.bundleCreator = new BundleCreator();
        this.payloadResolver = new PayloadResolver();
        //this.imagePublisher = new ImagePublisher(awsFactory);

        //this.terraformManager.provision();
    }

    private boolean isLocalStackEnvironment() {
        return Boolean.parseBoolean(System.getenv("IGNIS_LOCALSTACK_ENVIRONMENT"));
    }

    private Region resolveRegion() throws  ISchedulerException {
        if(isLocalStackEnvironment()) {
            return Region.US_EAST_1;
        }
        else{
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

    }

    private InstanceType resolveInstanceType(IClusterRequest driver) throws ISchedulerException {
        String type =  System.getenv("IGNIS_INSTANCE_TYPE");
        if(type != null && !type.isBlank()) {
            try {
                return InstanceType.fromValue(type.trim());
            }  catch (Exception e) {
                throw new ISchedulerException("Invalid instance type '" + type, e);
            }
        }

        int cpus = driver.resources().cpus();
        long ram = driver.resources().memory() / (1024L * 1024L);

        if (cpus <= 2 && ram <= 1024) return InstanceType.T3_MICRO;
        if (cpus <= 2 && ram <= 2048) return InstanceType.T3_SMALL;
        if (cpus <= 2 && ram <= 4096) return InstanceType.T3_MEDIUM;
        if (cpus <= 2 && ram <= 8192) return InstanceType.T3_LARGE;
        if (cpus <= 4 && ram <= 16384) return InstanceType.T3_XLARGE;
        if (cpus <= 8 && ram <= 32768) return InstanceType.T3_2_XLARGE;
        // TODO: analizar otras familias y requisitos MEJOR

        return InstanceType.T3_LARGE;
    }

    // Reference: [46], [47]
    private String resolveAMI() throws ISchedulerException {
        String userAMI = System.getenv("IGNIS_AMI");
        if(userAMI != null && !userAMI.isBlank()) return userAMI.trim();
        if(awsFactory.isLocalStackMode()) return "ami-0c55b159cbfafe1f0";

        String paramName = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64";

        try (SsmClient ssm = awsFactory.createSsmClient()) {
            return ssm.getParameter(GetParameterRequest.builder()
                            .name(paramName)
                            .build())
                    .parameter()
                    .value();
        } catch (Exception e) {
            throw new ISchedulerException("Failed to resolve AMI via SSM (" + paramName + ")", e);
        }
    }

    private Path baseDir() {
        String base = System.getenv("IGNIS_CLOUD_HOME");
        if (base != null && !base.isBlank()) {
            return Paths.get(base);
        }
        return Paths.get("/var/tmp/ignis-cloud");
    }

    private Path jobsDir() {
        return baseDir().resolve("jobs");
    }

    private void saveJobMeta(JobMeta meta) throws IOException {
        Path dir = jobsDir();
        Files.createDirectories(dir);

        Path file = dir.resolve(meta.jobId() + ".json");
        mapper.writeValue(file.toFile(), meta);
    }

    private JobMeta loadJobMetaFromDisk(String jobId) {
        try {
            Path file = jobsDir().resolve(jobId + ".json");
            if (!Files.exists(file)) {
                return null;
            }
            return mapper.readValue(file.toFile(), JobMeta.class);
        } catch (Exception e) {
            LOGGER.warn("Could not load job meta for {}", jobId, e);
            return null;
        }
    }

    private Map<String, JobMeta> loadAllJobsFromDisk() {
        Map<String, JobMeta> result = new HashMap<>();

        try {
            if (!Files.exists(jobsDir())) {
                return result;
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(jobsDir(), "*.json")) {
                for (Path p : stream) {
                    JobMeta meta = mapper.readValue(p.toFile(), JobMeta.class);
                    result.put(meta.jobId(), meta);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Could not load jobs from disk", e);
        }

        return result;
    }

    private void deleteJobMeta(String jobId) {
        try {
            Files.deleteIfExists(jobsDir().resolve(jobId + ".json"));
        } catch (IOException e) {
            LOGGER.warn("Could not delete job meta for {}", jobId);
        }
    }

    private synchronized void ensureInfrastructure() throws ISchedulerException {
        if (infrastructureReady) {
            return;
        }

        boolean runtime = Boolean.parseBoolean(System.getenv("IGNIS_CLOUD_RUNTIME"));
        if (runtime) {
            LOGGER.info("Cloud runtime mode detected: skipping infrastructure provisioning");
            return;
        }

        terraformManager.provision();
        infrastructureReady = true;
    }

    private String jobMetaKey(String jobId){
        return "jobs/" + jobId + "/job-meta.json";
    }

    private void saveJobMetaToS3(JobMeta meta) throws ISchedulerException {
        try{
            String json = mapper.writeValueAsString(meta);
            s3.putString(meta.bucket(), jobMetaKey(meta.jobId()), json, "application/json");
        } catch (Exception e){
            throw new ISchedulerException("Failed to upload job meta to S3 for job " + meta.jobId(), e);
        }
    }


    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {

        ensureInfrastructure();

        String jobId = ISchedulerUtils.genId().substring(0, 8);
        String finalJobName = name.replace("/", "-") + "-" + jobId;

        LOGGER.info("Creating job with name {} and id {}", finalJobName, jobId);

        String subnet = terraformManager.requireOutput("subnet_id");
        String sg = terraformManager.requireOutput("sg_id");
        //String iamRoleArn = terraformManager.requireOutput("iam_role_arn");
        String iamRoleArn="";
        String bucket = terraformManager.requireOutput("jobs_bucket_name");
        //String iamInstanceProfile = terraformManager.requireOutput("aws_iam_instance_profile");

        String iamInstanceProfile = System.getenv("IGNIS_IAM_INSTANCE_PROFILE");
        if (iamInstanceProfile == null || iamInstanceProfile.isBlank()) {
            throw new ISchedulerException("Missing IGNIS_IAM_INSTANCE_PROFILE (IAM creation disabled in this AWS account)");
        }

        if(subnet == null || sg == null || /*iamRoleArn == null ||*/ bucket == null) {
            throw new ISchedulerException("Terraform outputs not found");
        }

        try {
            List<IBindMount> binds = new ArrayList<>(payloadResolver.buildPayloadBindsFromArgs(driver));

            /*byte[] bundle = bundleCreator.createBundleTarGz(binds);
            String bundleKey = s3.uploadJobBundle(bucket, jobId, bundle);*/

            BundleResult result = bundleCreator.createBundleTarGzHybrid(binds, bucket, jobId, s3); // ← nuevo
            String bundleKey = s3.uploadJobBundle(bucket, jobId, result.tarGz()); // tar ligero


            String cmd = payloadResolver.resolveCommand(driver);
            //String image = payloadResolver.resolveImage(driver);
            String localImage = driver.resources().image();



            //String image = imagePublisher.ensurePublished(localImage); // TODO: esto entonces igual no es necesario (Correo Cesar)

            String image = driver.resources().image();

            // TODO: ECR sería aquí

            // TODO: despliegue en aws
            String userData = userDataBuilder.buildUserData(awsFactory.getRegion().id(),finalJobName, jobId,
                    bucket, bundleKey , image, cmd);

            InstanceType instanceType = resolveInstanceType(driver);

            String instanceId = ec2.createEC2Instance(finalJobName + "-driver", userData, resolveAMI(), subnet, sg, iamRoleArn, instanceType, iamInstanceProfile);

            //s3.downloadJob(jobId, bucket);

            int cpus = driver.resources().cpus();
            long memory = driver.resources().memory();
            String gpu =  driver.resources().gpu();
            List<String> args = driver.resources().args();

            JobMeta meta = new JobMeta(jobId, finalJobName, bucket, instanceId, image, cmd, cpus, memory, gpu, args);

            jobs.put(jobId, meta);


            //saveJobMeta(meta);
            saveJobMetaToS3(meta);


            LOGGER.info("Created job with name {} and id {}", finalJobName, jobId);
            //return finalJobName;
            return jobId;

        } catch (Exception e) {
            LOGGER.error("Error al interactuar con el SDK de AWS", e);
            throw new ISchedulerException("No se pudo lanzar la instancia EC2: " + e.getMessage(), e);
        }
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        LOGGER.info("Canceling job with id {}", id);

        JobMeta meta = jobs.get(id);
        if (meta == null) {
            meta = loadJobMetaFromDisk(id);
        }

        if (meta == null) {
            throw new ISchedulerException("job " + id + " not found");
        }

        try{
            try{
                String key = "jobs/" + id + "/status.json";
                String body = "{\"state\":\"DESTROYED\",\"rc\":143}";
                String type = "application/json";
                s3.putString(meta.bucket(), key, body,  type);
            } catch (Exception ignored){}

            String instanceId = meta.instanceId();
            if(instanceId != null && !instanceId.isBlank()) {
                    ec2.terminateInstance(instanceId);
            }
        }catch(Exception e){
            throw new ISchedulerException("Error canceling job " + id + " : " + e.getMessage(), e);
        }
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

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        LOGGER.info("Getting job with id {}", id);

        JobMeta meta = jobs.get(id);
        if (meta == null) {
            meta = loadJobMetaFromDisk(id);
            if(meta != null){
                jobs.put(id, meta);
            }
        }

        if (meta == null) {
            throw new ISchedulerException("job " + id + " not found");
        }

        try{
            /*IContainerInfo.IStatus status = statusFromS3(meta);
            System.out.println("Javi Rodriguez");
            if (status == null) {
                String awsState = ec2.getInstanceState(meta.instanceId());
                String key = (awsState != null) ? awsState.toLowerCase() : "not_found";
                status = CLOUD_STATUS.getOrDefault(key, IContainerInfo.IStatus.UNKNOWN);
            }*/

            IContainerInfo.IStatus status = statusFromS3(meta);
            if (status == null) {
                status = runtimeStatus.getOrDefault(id, IContainerInfo.IStatus.ACCEPTED);
                if(status == IContainerInfo.IStatus.ACCEPTED){
                    runtimeStatus.put(id, IContainerInfo.IStatus.RUNNING);
                }
            }

            if(statusFromS3(meta) == null){
                runtimeStatus.put(id, IContainerInfo.IStatus.ACCEPTED);
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

            /*return IJobInfo.builder()
                    .name(meta.jobName())
                    .id(meta.jobId())
                    .clusters(List.of(cluster))
                    .build();*/
            IJobInfo aux = IJobInfo.builder()
                    .name(meta.jobName())
                    .id(meta.jobId())
                    .clusters(List.of(cluster))
                    .build();

            runtimeStatus.put(meta.jobId(), IContainerInfo.IStatus.ACCEPTED);

            return aux;

        } catch (Exception e) {
                throw new ISchedulerException("Error getting job " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        //throw new UnsupportedOperationException();
        return List.of();
    }

    /*@Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        System.out.println("AAA: createCluster job=" + job + " instances=" + request.instances());

        IContainerInfo dummy = IContainerInfo.builder()
                .id("dummy-0")
                .node("dummy")
                .image("dummy")
                .args(List.of())
                .cpus(1)
                .gpu(null)
                .memory(1024L)
                .time(null)
                .user(null)
                .writable(true)
                .tmpdir(true)
                .ports(List.of())
                .binds(List.of())
                .nodelist(List.of())
                .hostnames(Map.of())
                .env(Map.of())
                .network(IContainerInfo.INetworkMode.HOST)
                .status(IContainerInfo.IStatus.ACCEPTED)
                .provider(IContainerInfo.IProvider.DOCKER)
                .schedulerOptArgs(Map.of())
                .build();

        return IClusterInfo.builder()
                .id("cluster-dummy")
                .instances(1)
                .containers(List.of(dummy))
                .build();
    }*/


    //############################## BACKUP DE CREATE CLUSTSER ##############################33
    /*
        @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        LOGGER.info("createCluster job {} instances={}", job, request.instances());

        String image = request.resources().image();
        int instances = request.instances();
        var containerIds = new ArrayList<String>();

        for (int i = 0; i < instances; i++) {
            String containerName = job + "-executor-" + i;
            try{
                List<String> cmd = new ArrayList<>();
                cmd.add(dockerBin);
                cmd.add("run");
                cmd.add("-d");
                //cmd.add("--rm"); -> Comentado Temporalmente
                cmd.add("--network"); cmd.add("host");
                //cmd.add("-p"); cmd.add("1963:1963");
                cmd.add("--name"); cmd.add(containerName);

                for(var entry : request.resources().env().entrySet()) {
                    String value = entry.getValue();
                    if(value != null) {
                        value = value.trim();
                    }
                    cmd.add("-e");  cmd.add(entry.getKey() + "=" + value);
                }

                cmd.add("-e"); cmd.add("IGNIS_SCHEDULER_ENV_JOB=" + job);
                cmd.add("-e"); cmd.add("IGNIS_SCHEDULER_ENV_CONTAINER=" + containerName);
                cmd.add("-e"); cmd.add("IGNIS_JOB_ID=" + job);
                cmd.add("-e"); cmd.add("IGNIS_JOB_CONTAINER_DIR=/opt/ignis/jobs");
                cmd.add("-e"); cmd.add("IGNIS_JOB_DIR=/opt/ignis/jobs/" + job);
                cmd.add("-v"); cmd.add("/ignis/dfs:/ignis/dfs");
                cmd.add("-v"); cmd.add("/var/run/docker.sock:/var/run/docker.sock");
                cmd.add("-v"); cmd.add("/opt/ignis/jobs/" + job + ":/opt/ignis/jobs/" + job);
                cmd.add(image);
                //cmd.add("ignis-executor");

                //List<String> executorArgs = request.resources().args();
                //if(executorArgs != null && !executorArgs.isEmpty()) {
                  //  cmd.addAll(executorArgs);
                //} else {
                    //cmd.add("ignis-run");
                //}

                List<String> executorArgs = new ArrayList<>();
                executorArgs.add("ignis-logger");
                if(request.resources().args() != null && !request.resources().args().isEmpty()) {
                    executorArgs.addAll(request.resources().args());
                } else{
                    executorArgs.add("ignis-run");
                }
                cmd.addAll(executorArgs);

                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                int rc = p.waitFor();
                if (rc != 0) {
                    String out = new String(p.getInputStream().readAllBytes());
                    throw new ISchedulerException("docker run failed for executor " + i + ": " + out);
                }
                containerIds.add(containerName);

                String publicKey = request.resources().env().get("IGNIS_CRYPTO_PUBLIC");
                if(publicKey != null) {
                    publicKey = publicKey.trim();

                    ProcessBuilder mkdirPb = new ProcessBuilder(dockerBin, "exec", containerName,
                            "bash", "-c",
                            "mkdir -p /root/.ssh && chmod 700 /root/.ssh && echo '" + publicKey + "' > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"
                    );

                    mkdirPb.redirectErrorStream(true);
                    Process mkdirP = mkdirPb.start();
                    mkdirP.waitFor();
                }

                # AQUI
                Thread.sleep(5000);

                ProcessBuilder logPb1 = new ProcessBuilder(dockerBin, "logs", containerName);
                logPb1.redirectErrorStream(true);
                Process logP1 = logPb1.start();
                System.out.println("S===== EXECUTOR LOG =====");
                System.out.println(new String(logP1.getInputStream().readAllBytes()));
                System.out.println("===== END EXECUTOR LOGS =====");

// Ver exit code
                ProcessBuilder inspectPb = new ProcessBuilder(dockerBin, "inspect",
                        "--format", "{{.State.ExitCode}} {{.State.Error}}", containerName);
                inspectPb.redirectErrorStream(true);
                Process inspectP = inspectPb.start();
                System.out.println("EXIT CODE: " + new String(inspectP.getInputStream().readAllBytes()));

                ProcessBuilder debugPb = new ProcessBuilder(dockerBin, "exec", containerName,
                        "cat", "/root/.ssh/authorized_keys");
                debugPb.redirectErrorStream(true);
                Process debugP = debugPb.start();
                System.out.println("AUTHORIZED_KEYS: " + new String(debugP.getInputStream().readAllBytes()));

                // ###############################################################################3
                // Capturar logs del executor para debug
                ProcessBuilder logPb = new ProcessBuilder(dockerBin, "logs", containerName);
                logPb.redirectErrorStream(true);
                Process logP = logPb.start();
                String logs = new String(logP.getInputStream().readAllBytes());
                System.out.println("===== EXECUTOR LOGS ("+ containerName +") =====");
                System.out.println(logs);
                System.out.println("===== END EXECUTOR LOGS ("+ containerName +") =====");
                // ###############################################################################3

                // Ver si el contenedor sigue vivo
                ProcessBuilder psPb = new ProcessBuilder(dockerBin, "ps", "-a", "--filter", "name=" + containerName, "--format", "{{.Names}} {{.Status}}");
                psPb.redirectErrorStream(true);
                Process psP = psPb.start();
                System.out.println("ESTADO EXECUTOR: " + new String(psP.getInputStream().readAllBytes()));

            } catch (Exception e){
                throw new ISchedulerException("Error launching executor container " + i + ": " + e.getMessage(), e);
            }
        }

        // Esperar a que el executor esté listo en el puerto 1963
        int maxWait = 30;
        boolean ready = false;
        for(int attempt = 0; attempt < maxWait; attempt++) {
            try(var socket = new Socket("localhost", 1963)){
                ready = true;
                break;
            } catch (Exception e) {
                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    throw new ISchedulerException("Failed to sleep after " + attempt + "s", e1);
                }

            }
        }

        if (!ready) {
            throw new ISchedulerException("Executor never became ready on port 1963");
        }

        var containers = new ArrayList<IContainerInfo>();
        for (int i = 0; i < containerIds.size(); i++) {
            containers.add(IContainerInfo.builder()
                    .id(containerIds.get(i))
                    .node("localhost")
                    .image(image)
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
                            "IGNIS_SCHEDULER_ENV_CONTAINER", containerIds.get(i)
                    ))
                    .network(IContainerInfo.INetworkMode.BRIDGE)
                    .status(IContainerInfo.IStatus.RUNNING)
                    .provider(IContainerInfo.IProvider.DOCKER)
                    .schedulerOptArgs(Map.of())
                    .build());
            // .ports(List.of(new IPortMapping(1963, 1963, IPortMapping.Protocol.TCP)))
        }

        return IClusterInfo.builder()
                .id(request.name())
                .instances(instances)
                .containers(containers)
                .build();
    }
     */

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        LOGGER.info("createCluster job {} instances={}", job, request.instances());

        String image = request.resources().image();
        int instances = request.instances();
        var containerIds = new ArrayList<String>();

        for (int i = 0; i < instances; i++) {
            String containerName = job + "-executor-" + i;
            try{
                List<String> cmd = new ArrayList<>();
                cmd.add(dockerBin);
                cmd.add("run");
                cmd.add("-d");
                cmd.add("--network"); cmd.add("host");
                cmd.add("--name"); cmd.add(containerName);

                for(var entry : request.resources().env().entrySet()) {
                    String value = entry.getValue();
                    if(value != null)  value = value.trim();
                    cmd.add("-e");  cmd.add(entry.getKey() + "=" + value);
                }

                cmd.add("-e"); cmd.add("IGNIS_SCHEDULER_ENV_JOB=" + job);
                cmd.add("-e"); cmd.add("IGNIS_SCHEDULER_ENV_CONTAINER=" + containerName);
                cmd.add("-e"); cmd.add("IGNIS_JOB_ID=" + job);
                cmd.add("-e"); cmd.add("IGNIS_JOB_CONTAINER_DIR=/opt/ignis/jobs");
                cmd.add("-e"); cmd.add("IGNIS_JOB_DIR=/opt/ignis/jobs/" + job);
                cmd.add("-v"); cmd.add("/ignis/dfs:/ignis/dfs");
                cmd.add("-v"); cmd.add("/var/run/docker.sock:/var/run/docker.sock");
                cmd.add("-v"); cmd.add("/opt/ignis/jobs/" + job + ":/opt/ignis/jobs/" + job);
                cmd.add(image);

                List<String> executorArgs = new ArrayList<>();
                executorArgs.add("ignis-logger");
                if(request.resources().args() != null && !request.resources().args().isEmpty()) {
                    executorArgs.addAll(request.resources().args());
                } else{
                    executorArgs.add("ignis-run");
                }
                cmd.addAll(executorArgs);

                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                int rc = p.waitFor();
                if (rc != 0) {
                    String out = new String(p.getInputStream().readAllBytes());
                    throw new ISchedulerException("docker run failed for executor " + i + ": " + out);
                }
                containerIds.add(containerName);

                String publicKey = request.resources().env().get("IGNIS_CRYPTO_PUBLIC");
                if(publicKey != null) {
                    publicKey = publicKey.trim();

                    ProcessBuilder mkdirPb = new ProcessBuilder(dockerBin, "exec", containerName,
                            "bash", "-c",
                            "mkdir -p /root/.ssh && chmod 700 /root/.ssh && echo '" + publicKey + "' > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"
                    );

                    mkdirPb.redirectErrorStream(true);
                    Process mkdirP = mkdirPb.start();
                    mkdirP.waitFor();
                }

                Thread.sleep(5000);

            } catch (Exception e){
                throw new ISchedulerException("Error launching executor container " + i + ": " + e.getMessage(), e);
            }
        }

        // Esperar a que el executor esté listo en el puerto 1963
        int maxWait = 30;
        boolean ready = false;
        for(int attempt = 0; attempt < maxWait; attempt++) {
            try(var socket = new Socket("localhost", 1963)){
                ready = true;
                break;
            } catch (Exception e) {
                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    throw new ISchedulerException("Failed to sleep after " + attempt + "s", e1);
                }

            }
        }

        if (!ready) {
            throw new ISchedulerException("Executor never became ready on port 1963");
        }

        var containers = new ArrayList<IContainerInfo>();
        for (int i = 0; i < containerIds.size(); i++) {
            containers.add(IContainerInfo.builder()
                    .id(containerIds.get(i))
                    .node("localhost")
                    .image(image)
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
                            "IGNIS_SCHEDULER_ENV_CONTAINER", containerIds.get(i)
                    ))
                    .network(IContainerInfo.INetworkMode.BRIDGE)
                    .status(IContainerInfo.IStatus.RUNNING)
                    .provider(IContainerInfo.IProvider.DOCKER)
                    .schedulerOptArgs(Map.of())
                    .build());
        }

        return IClusterInfo.builder()
                .id(request.name())
                .instances(instances)
                .containers(containers)
                .build();
    }


    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
        System.out.println("AAA: destroyCluster");
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        System.out.println("AAA: getCluster");
        return null;
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException {
        System.out.println("AAA: repairCluster");
        return cluster;
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        System.out.println("AAA: getContainerStatus");
        /*String awsState = ec2.getInstanceState(id);
        if (awsState == null) {
            return IContainerInfo.IStatus.UNKNOWN;
        }
        System.out.println("AAA: getContainerStatus");
        return CLOUD_STATUS.getOrDefault(awsState.toLowerCase(), IContainerInfo.IStatus.UNKNOWN);

         */

        // Si es un docker container
        if(!id.startsWith("i-")) {
            try{
                ProcessBuilder pb = new ProcessBuilder(dockerBin, "inspect", "--format", "{{.State.Status}}", id);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                String output = new String(p.getInputStream().readAllBytes());
                p.waitFor();
                System.out.println("AAA: getContainerStatus id=" + id + " status=" + output);
                String outputUpper = output.trim().toUpperCase();
                return switch (outputUpper) {
                    case "CREATED" -> IContainerInfo.IStatus.ACCEPTED;
                    case "RUNNING" -> IContainerInfo.IStatus.RUNNING;
                    case "EXITED"  -> IContainerInfo.IStatus.FINISHED;
                    default -> IContainerInfo.IStatus.UNKNOWN;
                };
            } catch(Exception e){
                return IContainerInfo.IStatus.UNKNOWN;
            }
        }

        // Si es instancia EC2
        String awsState = ec2.getInstanceState(id);
        if(awsState == null) return IContainerInfo.IStatus.UNKNOWN;
        return CLOUD_STATUS.getOrDefault(awsState.toLowerCase(), IContainerInfo.IStatus.UNKNOWN);
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        System.out.println("AAA: healthCheck");
    }
}