package org.ignis.scheduler;

import org.ignis.scheduler.model.IContainerInfo;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class EC2Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private final AwsFactory awsFactory;
    private final String defaultAmi = "ami-0c55b159cbfafe1f0";
    private final InstanceType defaultInstanceType = InstanceType.T2_MICRO;

    private static final Map<String, IContainerInfo.IStatus> EC2_STATUS_MAP = Map.ofEntries(
            Map.entry("pending",       IContainerInfo.IStatus.ACCEPTED),
            Map.entry("running",       IContainerInfo.IStatus.RUNNING),
            Map.entry("stopping",      IContainerInfo.IStatus.RUNNING),
            Map.entry("stopped",       IContainerInfo.IStatus.FINISHED),
            Map.entry("shutting-down", IContainerInfo.IStatus.DESTROYED),
            Map.entry("terminated",    IContainerInfo.IStatus.DESTROYED)
    );

    public EC2Operations(AwsFactory awsFactory) {
        this.awsFactory = awsFactory;
    }

    // Reference [19], [22], [23]
    public String createEC2Instance(String instanceName, String userDataScript, String amiId, String subnet, String sgId, String iam) throws ISchedulerException {
        Ec2Client ec2 = awsFactory.createEc2Client();
        try {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(defaultAmi)
                    .instanceType(defaultInstanceType)
                    .maxCount(1)
                    .minCount(1)
                    .subnetId(subnet)
                    .securityGroupIds(sgId)
                    /*.iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .arn(iam)
                            .build())*/
                    /*TODO: analizar*/              .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8)))
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(ResourceType.INSTANCE)
                            .tags(Tag.builder().key("Name").value(instanceName).build(),
                                    Tag.builder().key("JobName").value(instanceName.split("-")[0]).build())
                            .build())
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId(); // TODO: se pueden lanzar múltiples instancias? Debería permitirlo?

            LOGGER.info("Instance launched: {}", instanceId);
            return instanceId;
        }catch (Exception e){
            LOGGER.error("Failed to create EC2 instance", e);
            throw new ISchedulerException("Failed to create EC2 instance", e);
        }
    }

    // Reference: [19]
    public void stopInstance(Ec2Client ec2, String instanceId) throws ISchedulerException {
        if (instanceId == null || instanceId.trim().isEmpty()){
            throw new ISchedulerException("Instance id can't be null or empty");
        }
        try {
            StopInstancesRequest request = StopInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.stopInstances(request);
            LOGGER.info("Successfully stopped instance: {}", instanceId);
        } catch (Exception e){
            LOGGER.error("Failed to stop instance", e);
            throw new ISchedulerException("Failed to stop instance", e);
        }

    }

    // Reference: [40]
    public void terminateInstance(Ec2Client ec2, String instanceId) throws ISchedulerException {
        if (instanceId == null || instanceId.trim().isEmpty()){
            throw new ISchedulerException("Instance id can't be null or empty");
        }
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.terminateInstances(request);
            LOGGER.info("Successfully terminated instance: {}", instanceId);
        } catch (Exception e){
            LOGGER.error("Failed to terminate instance", e);
            throw new ISchedulerException("Failed to terminate instance", e);
        }
    }

    // Reference: [40]
    private void rebootInstance(Ec2Client ec2, String instanceId) throws ISchedulerException {
        if (instanceId == null || instanceId.trim().isEmpty()){
            throw new ISchedulerException("Instance id can't be null or empty");
        }
        try {
            RebootInstancesRequest request = RebootInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.rebootInstances(request);
        } catch (Exception e){
            LOGGER.error("Failed to reboot instance", e);
            throw new ISchedulerException("Failed to reboot instance", e);
        }
    }

    // Reference: [40]
    public Instance getInstanceInfo(String instanceId) throws ISchedulerException {
        if (instanceId == null || instanceId.trim().isEmpty()){
            throw new ISchedulerException("Instance id can't be null or empty");
        }

        Ec2Client ec2 = awsFactory.createEc2Client();

        try {
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

    public IContainerInfo.IStatus mapToSchedulerStatus(String awsState) {
        if (awsState == null) return IContainerInfo.IStatus.UNKNOWN;
        String key = awsState.trim().toLowerCase();
        return EC2_STATUS_MAP.getOrDefault(key, IContainerInfo.IStatus.UNKNOWN);
    }

}
