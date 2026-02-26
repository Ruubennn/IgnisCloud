package org.ignis.scheduler;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

public class AwsFactory {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final Region DEFAULT_REGION = Region.US_WEST_2;
    private static final String LOCALSTACK_ENDPOINT = "http://localhost:4566";

    private final boolean isLocalStackMode;
    private final URI localStackUri;

    public AwsFactory() {
        this(isLocalStackEnvironment());
    }

    public AwsFactory(boolean isLocalStackMode) {
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
    }

    // TODO: Revisar y configurar bien para cuando sea el despliegue en AWS
    private static boolean isLocalStackEnvironment() {

        /*if ("true".equalsIgnoreCase(System.getenv("LOCALSTACK"))) {
            return true;
        }

        if (Boolean.getBoolean("aws.localstack.enabled")) {
            return true;
        }

        return false;*/
        return true;
    }

    public boolean isLocalStackMode() {
        return isLocalStackMode;
    }

    public Region getRegion() {
        return DEFAULT_REGION;
    }

    public Ec2Client createEc2Client() {
        var builder = Ec2Client.builder().region(getRegion());
        if (isLocalStackMode()) {
            builder
                    .endpointOverride(localStackUri)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));

        }
        return builder.build();
    }

    public S3Client createS3Client() {
        var builder = S3Client.builder().region(getRegion());
        if (isLocalStackMode()) {
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
}


