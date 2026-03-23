package org.ignis.scheduler;

import org.ignis.scheduler.model.IClusterRequest;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class EC2Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(EC2Operations.class);
    private final AwsFactory awsFactory;

    public EC2Operations(AwsFactory awsFactory) {
        this.awsFactory = awsFactory;
    }

    // Reference [19], [22], [23]
    public String createEC2Instance(String instanceName, String userDataScript, String amiId, String subnet, String sgId, String iam, InstanceType instanceType, String iamInstanceProfile) throws ISchedulerException {
        try (Ec2Client ec2 = awsFactory.createEc2Client()){
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
        try (Ec2Client ec2 = awsFactory.createEc2Client()){
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
        try (Ec2Client ec2 = awsFactory.createEc2Client()){
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
            LOGGER.error("Instance not found: {}", instanceId);
            return null;

        } catch (Ec2Exception e){
            LOGGER.error("Failed to get instance info", e);
            return null;
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
}
