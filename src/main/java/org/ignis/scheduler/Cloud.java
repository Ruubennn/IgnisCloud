package org.ignis.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ignis.scheduler.model.*;
import java.io.*;
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

        this.awsFactory = new AwsFactory(isLocalStackEnvironment(), resolveRegion());
        this.terraformManager = new TerraformManager(awsFactory.getRegion().id());
        this.ec2 = new EC2Operations(awsFactory);
        this.s3 = new S3Operations(awsFactory);
        this.userDataBuilder = new UserDataBuilder();
        this.bundleCreator = new BundleCreator();
        this.payloadResolver = new PayloadResolver();

        this.terraformManager.provision();
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

    private Path jobsDir() {
        return Paths.get(System.getProperty("user.home"), ".ignis-cloud", "jobs");
    }

    private void saveJobMeta(JobMeta meta) throws IOException {
        Files.createDirectories(jobsDir());
        Path file = jobsDir().resolve(meta.jobId() + ".json");
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


    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {
        System.out.println("Test 1");
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

        System.out.println("Test 2");
        if(subnet == null || sg == null || /*iamRoleArn == null ||*/ bucket == null) {
            throw new ISchedulerException("Terraform outputs not found");
        }

        try {
            System.out.println("Test 3");
            List<IBindMount> binds = new ArrayList<>(payloadResolver.buildPayloadBindsFromArgs(driver));

            String jarlibs = System.getenv("IGNIS_JARLIBS_PATH");
            if (jarlibs != null && !jarlibs.isBlank()) {
                Path jarsPath = Paths.get(jarlibs);
                if (!Files.exists(jarsPath) || !Files.isDirectory(jarsPath)) {
                    throw new ISchedulerException("IGNIS_JARLIBS_PATH does not exist or is not a directory: " + jarlibs);
                }

                binds.add(new IBindMount("/ignis/dfs/jarlibs", jarlibs, true));
            }

            byte[] bundle = bundleCreator.createBundleTarGz(binds);
            String bundleKey = s3.uploadJobBundle(bucket, jobId, bundle);
            String cmd = payloadResolver.resolveCommand(driver);
            String image = payloadResolver.resolveImage(driver);
            System.out.println("Test 4");

            // TODO: despliegue en aws
            String userData = userDataBuilder.buildUserData(awsFactory.getRegion().id(),finalJobName, jobId,
                    bucket, bundleKey , image, cmd);
            System.out.println("Test 5");

            InstanceType instanceType = resolveInstanceType(driver);
            System.out.println("Test 6");
            String instanceId = ec2.createEC2Instance(finalJobName + "-driver", userData, resolveAMI(), subnet, sg, iamRoleArn, instanceType, iamInstanceProfile);
            System.out.println("Test 7");
            //s3.downloadJob(jobId, bucket);

            int cpus = driver.resources().cpus();
            long memory = driver.resources().memory();
            String gpu =  driver.resources().gpu();
            List<String> args = driver.resources().args();
            System.out.println("Test 8");

            JobMeta meta = new JobMeta(jobId, finalJobName, bucket, instanceId, image, cmd, cpus, memory, gpu, args);
            System.out.println("Test 9");
            jobs.put(jobId, meta);
            saveJobMeta(meta);

            System.out.println("Salió del createJOB");
            return finalJobName;

        } catch (Exception e) {
            LOGGER.error("Error al interactuar con el SDK de AWS", e);
            throw new ISchedulerException("No se pudo lanzar la instancia EC2: " + e.getMessage(), e);
        }
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        LOGGER.info("Canceling job with id {}", id);

        JobMeta meta = jobs.get(id);
        meta = (meta != null) ? meta : loadJobMetaFromDisk(id);

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
        meta = (meta != null) ? meta : loadJobMetaFromDisk(id);

        if (meta == null) {
            throw new ISchedulerException("job " + id + " not found");
        }
        try{
            IContainerInfo.IStatus status = statusFromS3(meta);

            if (status == null) {
                String awsState = ec2.getInstanceState(meta.instanceId());
                String key = (awsState != null) ? awsState.toLowerCase() : "not_found";
                status = CLOUD_STATUS.getOrDefault(key, IContainerInfo.IStatus.UNKNOWN);
            }

            IContainerInfo container = IContainerInfo.builder()
                    .id(meta.instanceId())
                    .node(meta.instanceId())
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
                    .network(IContainerInfo.INetworkMode.HOST)
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
        return null;
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        String awsState = ec2.getInstanceState(id);
        if (awsState == null) {
            return IContainerInfo.IStatus.UNKNOWN;
        }
        System.out.println("AAA: getContainerStatus");
        return CLOUD_STATUS.getOrDefault(awsState.toLowerCase(), IContainerInfo.IStatus.UNKNOWN);
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        System.out.println("AAA: healthCheck");
    }
}