package org.ignis.scheduler;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class EC2Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private final AwsFactory awsFactory;
    private final String defaultAmi = "ami-0c55b159cbfafe1f0";
    private final InstanceType defaultInstanceType = InstanceType.T2_MICRO;

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
}
