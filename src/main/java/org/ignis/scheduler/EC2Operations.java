package org.ignis.scheduler;

import org.ignis.scheduler.model.IClusterRequest;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class EC2Operations implements Closeable {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(EC2Operations.class);
    private final AwsFactory awsFactory;
    private final Ec2Client ec2;
    private final SsmClient ssm;

    public EC2Operations(Ec2Client ec2, SsmClient ssm, AwsFactory awsFactory) {
        this.ec2 = ec2;
        this.ssm = ssm;
        this.awsFactory = awsFactory;
    }

    // Reference [19], [22], [23]
    public String createEC2Instance(String instanceName, String userDataScript, String amiId, String subnet, String sgId, String iam, InstanceType instanceType, String iamInstanceProfile) throws ISchedulerException {
        try{
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(instanceType)
                    .maxCount(1)
                    .minCount(1)
                    .subnetId(subnet)
                    .securityGroupIds(sgId)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .name(iamInstanceProfile)
                            .build())
                    .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
                    .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8)))
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(ResourceType.INSTANCE)
                            .tags(Tag.builder().key("Name").value(instanceName).build(),
                                    Tag.builder().key("JobName").value(instanceName.split("-")[0]).build())
                            .build())
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId();

            LOGGER.info("Instance launched: {}", instanceId);
            return instanceId;
        } catch (Ec2Exception e) {
            LOGGER.error("Failed to create EC2 instance. AWS error: {} - {}",
                    e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "unknown",
                    e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(),
                    e);
            throw new ISchedulerException("Failed to create EC2 instance: " +
                    (e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage()), e);
        }
    }

    // Reference: [40]
    public void terminateInstance(String instanceId) throws ISchedulerException {
        try{
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            ec2.terminateInstances(request);
            LOGGER.info("Successfully terminated instance: {}", instanceId);
        } catch (Ec2Exception e){
            String code = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null;
            if ("InvalidInstanceID.NotFound".equals(code)) return;
            throw new ISchedulerException("Failed to terminate instance " + instanceId, e);
        }
    }

    // Reference: [40]
    public Instance getInstanceInfo(String instanceId) throws ISchedulerException {
        if (instanceId == null || instanceId.trim().isEmpty()){
            throw new ISchedulerException("Instance id can't be null or empty");
        }
        try{
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                        if(instanceId.equals(instance.instanceId())) {
                            return instance;
                        }
                }
            }
            LOGGER.debug("Instance not found: {}", instanceId);
            return null;

        } catch (Ec2Exception e) {
            if ("i-0000000000000".equals(instanceId)) {
                LOGGER.debug("Ignoring error for dummy instance: {}", e.getMessage());
                return null;
            }
            LOGGER.error("Failed to get instance info for {}: {}", instanceId, e.getMessage());
            throw new ISchedulerException("Failed to get instance info for " + instanceId, e);
        }
    }

    public String getInstanceState(String instanceId) throws ISchedulerException {
        Instance inst = getInstanceInfo(instanceId);
        if (inst == null) {
            return "not_found";
        }
        return inst.state().nameAsString().toLowerCase();
    }


    public InstanceType resolveInstanceType(IClusterRequest driver) throws ISchedulerException {
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
    public String resolveAMI() throws ISchedulerException {
        String userAMI = System.getenv("IGNIS_AMI");
        if(userAMI != null && !userAMI.isBlank()) return userAMI.trim();

        String paramName = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64";

        try{
            return ssm.getParameter(GetParameterRequest.builder()
                            .name(paramName)
                            .build())
                    .parameter()
                    .value();
        } catch (Exception e) {
            throw new ISchedulerException("Failed to resolve AMI via SSM (" + paramName + ")", e);
        }
    }

    public String resolveAvailabilityZone() throws ISchedulerException {
        // 1. Env var
        String configuredAZ = System.getenv("IGNIS_AWS_AZ");
        if(configuredAZ != null && !configuredAZ.isBlank()) {
            LOGGER.info("Using AZ from IGNIS_AWS_AZ: {}", configuredAZ);
            return configuredAZ.trim();
        }

        // 2. Search available AZs at the region
        try{
            DescribeAvailabilityZonesRequest request = DescribeAvailabilityZonesRequest.builder()
                    .filters(Filter.builder()
                            .name("state")
                            .values("available")
                            .build())
                    .build();

            List<AvailabilityZone> zones = ec2.describeAvailabilityZones(request).availabilityZones();
            if(!zones.isEmpty()) {
                String az = zones.get(0).zoneName();
                LOGGER.info("Auto-resolved AZ for region {}: {}", awsFactory.getRegion(), az);
                return az;
            }
        } catch (Exception e) {
            LOGGER.warn("Could not auto-resolve AZ from AWS, falling back to default", e);
        }

        // 3. Fallback
        String fallback = awsFactory.getRegion().id() + "a";
        LOGGER.info("Using fallback AZ: {}", fallback);
        return fallback;
    }

    @Override
    public void close() {
        ec2.close();
        ssm.close();
    }
}
