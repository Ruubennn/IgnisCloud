package org.ignis.scheduler;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.ssm.SsmClient;

import java.net.URI;

public class AwsFactory {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);
    private static final String LOCALSTACK_ENDPOINT = "http://localhost:4566";

    private final Region region;
    private final boolean isLocalStackMode;
    private final URI localStackUri;

    public AwsFactory(boolean isLocalStackMode, Region region) {
        this.isLocalStackMode = isLocalStackMode;
        if (isLocalStackMode) {
            try {
                this.localStackUri = URI.create(LOCALSTACK_ENDPOINT);
                LOGGER.info("Using local stack endpoint {}", this.localStackUri);
            }catch (Exception e) {
                throw new IllegalStateException("Local stack endpoint not found", e);
            }
        } else {
            this.localStackUri = null;
            LOGGER.info("Using AWS");
        }
        this.region = region;
    }

    public boolean isLocalStackMode() {
        return isLocalStackMode;
    }

    public Region getRegion() {
        return region;
    }

    // Reference: [20], [21]
    public Ec2Client createEc2Client() {
        var builder = Ec2Client.builder(); //.region(getRegion());

        if(getRegion() != null) {
            builder.region(getRegion());
        }

        if (isLocalStackMode()) {
            if(getRegion() == null) builder.region(Region.US_WEST_2);
            builder.endpointOverride(localStackUri)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));

        }
        return builder.build();
    }

    // Reference: [28]
    public S3Client createS3Client() {
        var builder = S3Client.builder();
        if(getRegion() != null) {
            builder.region(getRegion());
        }
        if (isLocalStackMode()) {
            if(getRegion() == null) builder.region(Region.US_WEST_2);
            builder
                    .endpointOverride(localStackUri)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                    .serviceConfiguration(
                            S3Configuration.builder()
                                    .pathStyleAccessEnabled(true)     // <-- CLAVE para LocalStack
                                    .chunkedEncodingEnabled(false)    // <-- evita problemas de chunked
                                    .build());
        }
        return builder.build();
    }

    public SsmClient createSsmClient(){
        var builder =  SsmClient.builder();
        if(getRegion() != null) {
            builder.region(getRegion());
        }
        if (isLocalStackMode()) {
            if(getRegion() == null) builder.region(Region.US_WEST_2);
            builder
                    .endpointOverride(localStackUri)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
        }
        return builder.build();
    }
}


