package org.ignis.scheduler;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class ImagePublisher {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ImagePublisher.class);

    private final AwsFactory awsFactory;

    public ImagePublisher(AwsFactory awsFactory) {
        this.awsFactory = awsFactory;
    }

    // Reference: [49],
    public String ensurePublished(String localImage) throws ISchedulerException {
        if(localImage == null || localImage.isBlank()){
            throw new ISchedulerException("Local image is null or empty");
        }

        assertLocalImageExists(localImage);

        String repositoryName = toRepositoryName(localImage);
        String imageId = resolveLocalId(localImage);
        String imageTag = "img-" + shortenImageId(imageId);

        try(EcrClient ecr = awsFactory.createECRClient()) {
            String repositoryUri = ensureRepositoryExists(ecr, repositoryName);
            String remoteImage = repositoryUri + ":" + imageId;

            if(imageAlreadyExists(ecr, repositoryName, imageTag)){
                LOGGER.info("Image already exists in ECR: {}", remoteImage);
                return remoteImage;
            }

            dockerLoginToEcr(ecr);

            runCommand(List.of("docker", "tag", localImage, remoteImage), null);
            runCommand(List.of("docker", "push", remoteImage), null);

            LOGGER.info("Published image {} -> {}", localImage, remoteImage);
            return remoteImage;
        } catch (ISchedulerException e) {
            throw e;
        } catch (Exception e) {
            throw new ISchedulerException("Failed to publish image to ECR: " + localImage, e);
        }
    }

    // TODO: no sé muy bien cuál es su función --> Comprobar si falla?
    private void assertLocalImageExists(String localImage) throws ISchedulerException {
        runCommand(List.of("docker", "image", "inspect", localImage), null);
    }

    // Normalize the image to be a valid ECR sintax
    private String toRepositoryName(String localImage) throws ISchedulerException {
        String name = localImage.trim();

        int slashAt =  name.lastIndexOf('/');
        int colonAt =  name.lastIndexOf(':');

        if(colonAt > slashAt){
            name = name.substring(0, colonAt);
        }

        return name.replace('/', '-').replace(':', '-').toLowerCase();
    }

    // To obtain a unique identifier
    private String resolveLocalId(String localImage) throws ISchedulerException {
        String out = runCommand(List.of("docker", "image", "inspect", "--format", "{{.Id}}", localImage), null).trim();
        if(out.isBlank()) throw new ISchedulerException("Could not resolve Docker image ID for " + localImage);
        return out;
    }

    // To make the image more readable
    private String shortenImageId(String imageId) throws ISchedulerException {
        String clean = imageId.replace("sha256:","").trim();
        return clean.length() > 12 ? clean.substring(0, 12) : clean;
    }

    // Reference: [50] (Learn the basics), (CreateRepository)
    private String ensureRepositoryExists(EcrClient ecr, String repositoryName) {
        try {
            return ecr.describeRepositories(
                            DescribeRepositoriesRequest.builder()
                                    .repositoryNames(repositoryName)
                                    .build()
                    )
                    .repositories()
                    .get(0)
                    .repositoryUri();
        } catch (RepositoryNotFoundException e) { // Creates repository if not exists
            CreateRepositoryResponse created = ecr.createRepository(
                    CreateRepositoryRequest.builder()
                            .repositoryName(repositoryName)
                            .imageTagMutability(ImageTagMutability.IMMUTABLE)
                            .build()
            );
            return created.repository().repositoryUri();
        }
    }

    // Reference: [50] (DescribeImages)
    private boolean imageAlreadyExists(EcrClient ecr, String repositoryName, String imageTag) {
        try {
            ecr.describeImages(
                    DescribeImagesRequest.builder()
                            .repositoryName(repositoryName)
                            .imageIds(ImageIdentifier.builder().imageTag(imageTag).build())
                            .build()
            );
            return true;
        } catch (ImageNotFoundException e) {
            return false;
        }
    }

    // Reference: [51]
    private void dockerLoginToEcr(EcrClient ecr) throws ISchedulerException {
        AuthorizationData auth = ecr.getAuthorizationToken(GetAuthorizationTokenRequest.builder().build())
                .authorizationData()
                .stream()
                .findFirst()
                .orElseThrow(() -> new ISchedulerException("ECR returned no authorization data"));

        String proxyEndpoint = auth.proxyEndpoint();
        String decoded = new String(
                Base64.getDecoder().decode(auth.authorizationToken()),
                StandardCharsets.UTF_8
        );

        String[] parts = decoded.split(":", 2);
        if (parts.length != 2) {
            throw new ISchedulerException("Invalid ECR authorization token format");
        }

        String password = parts[1];

        runCommand(
                List.of("docker", "login", "--username", "AWS", "--password-stdin", proxyEndpoint),
                password
        );
    }

    private String runCommand(List<String> command, String stdin) throws ISchedulerException {
        try{
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);

            Process process = pb.start();

            if(stdin != null){ // TODO: esta parte es realmente necesaria????
                try(OutputStream os = process.getOutputStream()){
                    os.write(stdin.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
            } else{
                process.getInputStream().close();
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            process.getInputStream().transferTo(baos); // TODO: esta parte es realmente necesaria????

            int exit = process.waitFor();
            String output = baos.toString(StandardCharsets.UTF_8);

            if (exit != 0) {
                throw new ISchedulerException(
                        "Command failed (" + String.join(" ", command) + "): " + output
                );
            }
            return output;

        } catch (IOException e) {
            throw new ISchedulerException("I/O error running command: " + String.join(" ", command), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Interrupted running command: " + String.join(" ", command), e);
        }
    }
}
