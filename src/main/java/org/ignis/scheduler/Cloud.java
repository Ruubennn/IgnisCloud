package org.ignis.scheduler;

import org.ignis.scheduler.model.*;
import java.io.*;
import java.util.*;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.*;

public class Cloud implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private final TerraformManager terraformManager;
    private final AwsFactory awsFactory;
    private final EC2Operations ec2;
    private final S3Operations s3;
    private final UserDataBuilder userDataBuilder;
    private final BundleCreator bundleCreator;
    private final PayloadResolver payloadResolver;

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

        this.terraformManager = new TerraformManager();
        this.awsFactory = new AwsFactory();
        this.ec2 = new EC2Operations(awsFactory);
        this.s3 = new S3Operations(awsFactory);
        this.userDataBuilder = new UserDataBuilder();
        this.bundleCreator = new BundleCreator();
        this.payloadResolver = new PayloadResolver();

        this.terraformManager.provision();
    }
    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {

        String jobId = ISchedulerUtils.genId().substring(0, 8);
        String finalJobName = name.replace("/", "-") + "-" + jobId;

        LOGGER.info("Creating job with name {} and id {}", finalJobName, jobId);

        String subnet = terraformManager.requireOutput("subnet_id");
        String sg = terraformManager.requireOutput("sg_id");
        String iamRoleArn = terraformManager.requireOutput("iam_role_arn");
        String bucket = terraformManager.requireOutput("jobs_bucket_name");

        if(subnet == null || sg == null || iamRoleArn == null || bucket == null) {
            throw new ISchedulerException("Terraform outputs not found");
        }

        try {

            String scriptPath = payloadResolver.resolveCloudScriptPath(driver);
            if (scriptPath == null) {
                throw new ISchedulerException("Script path not found");
            }
            List<IBindMount> binds = payloadResolver.buildPayloadBindsFromArgs(driver);
            byte[] bundle = bundleCreator.createBundleTarGz(binds);
            String bundleKey = s3.uploadJobBundle(bucket, jobId, bundle);

            String cmd = "python3 " + scriptPath;

            /*String userData = buildUserData(finalJobName, bucket, bundleKey, jobId,
                    driver.resources().image(), cmd);*/
            // TODO: despliegue en aws
            String userData = userDataBuilder.buildUserData(finalJobName, jobId, bucket, bundleKey ,"python:3.11-slim", cmd);
            
            //String userData = buildUserData(finalJobName, bucket, bundleKey, jobId, driver.resources().image(), driver.resources().args());
            String instanceId = ec2.createEC2Instance(finalJobName + "-driver", userData, "ami-df570af1", subnet, sg, iamRoleArn);

            s3.downloadJob(jobId, bucket);

            return finalJobName;

        } catch (Exception e) {
            LOGGER.error("Error al interactuar con el SDK de AWS", e);
            throw new ISchedulerException("No se pudo levantar la instancia en LocalStack: " + e.getMessage());
        }
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        System.out.println("Canceling job with id " + id);
        System.out.println("AAAAAAAAAAAAAAAAAaaaaaaaaAAAAAAAAa");
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        System.out.println("AAA: getJob");
        return null;
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        System.out.println("AAA: listJobs");
        return null;
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        System.out.println("AAA: createCluster");
        return null;
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
        return CLOUD_STATUS.getOrDefault(awsState.toLowerCase(), IContainerInfo.IStatus.UNKNOWN);
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        System.out.println("AAA: healthCheck");
    }
}