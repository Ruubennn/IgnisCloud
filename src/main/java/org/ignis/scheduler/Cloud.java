package org.ignis.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ignis.scheduler.IScheduler;
import org.ignis.scheduler.ISchedulerException;
import org.ignis.scheduler.ISchedulerUtils;
import org.ignis.scheduler.model.IClusterInfo;
import org.ignis.scheduler.model.IClusterRequest;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IJobInfo;

import java.io.*;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI; // Necesario para LocalStack
import java.util.stream.Stream;

public class Cloud implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final String TF_BIN_PROP = "ignis.terraform.bin";
    private static final String TF_RESOURCE_DIR = "terraform";
    private static boolean CLEANUP_WORKDIR = true;
    private Map<String, String> terraformOutputs = new HashMap<>();

    public Cloud(String url) throws ISchedulerException{

        System.out.println("\n*****************************************");
        System.out.println("URL: " + url);
        System.out.println("*****************************************\n");

        LOGGER.info("Initializing Cloud scheduler at: {}", url);

        runTerraform();

    }

    private void runTerraform()  throws ISchedulerException {
        LOGGER.info("Starting infrastructure provisioning via Terraform...");

        String terraformBin = System.getProperty(TF_BIN_PROP, "terraform");
        Path workDir = null;

        try {
            workDir = Files.createTempDirectory("ignis-terraform-");
            LOGGER.debug("Creating temporary directory: {}", workDir.toString());

            copyClasspathDirectoryTo(workDir);

            executeCommand(workDir.toString(), terraformBin, "init", "-input=false");
            executeCommand(workDir.toString(), terraformBin, "apply", "-auto-approve", "-input=false");

            captureTerraformOutputs(workDir.toString(), terraformBin);

            LOGGER.info("Terraform infrastructure applied successfully.");
        } catch (ISchedulerException e){
            LOGGER.error("Failed to provision infrastructure: {}", e.getMessage());
            throw e;
        } catch (IOException e){
            LOGGER.error("Failed to prepare or cleanup Terraform directory", e);
            throw new ISchedulerException("Failed to prepare or cleanup Terraform directory", e);
        } finally {
            if (CLEANUP_WORKDIR && workDir != null && Files.exists(workDir)) {
                try{
                    LOGGER.info("Cleaning up temporary directory: {}", workDir.toString());
                    deleteDirectoryRecursively(workDir);
                } catch (IOException e){
                    LOGGER.warn("Cleanup failed for {}", workDir, e);
                }
            }
        }
    }

    private void copyClasspathDirectoryTo(Path destination) throws IOException {
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource(TF_RESOURCE_DIR);
        if (url == null) {
            throw new IOException("Cannot find resource " + TF_RESOURCE_DIR);
        }
        if (!url.getProtocol().equals("jar")) {
            throw new IOException("Cannot find resource " + TF_RESOURCE_DIR);
        }
        String jarPath = url.toString().split("!")[0].substring("jar:".length());
        String prefix = TF_RESOURCE_DIR + "/";
        jarPath = jarPath.substring(5);

        String urlStr = url.toString();
        String jarUrlPart = urlStr.substring(0, urlStr.indexOf("!/"));

        if (jarUrlPart.startsWith("jar:file:")) {
            jarPath = jarUrlPart.substring("jar:file:".length());
        } else {
            throw new IOException("Unexpected JAR URL format: " + urlStr);
        }

        try(JarFile jar = new JarFile(jarPath)) {
            jar.stream()
                    .filter(entry -> entry.getName().startsWith(prefix))
                    .forEach(entry -> {
                       try {
                           String relPath = entry.getName().substring(prefix.length());
                           if(relPath.isEmpty()) {
                               return;
                           }
                           Path target = destination.resolve(relPath);
                           if(entry.isDirectory()) {
                               Files.createDirectories(target);
                           } else {
                               Files.createDirectories(target.getParent());
                               try (InputStream is = jar.getInputStream(entry)) {
                                   Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);
                               }
                           }
                       } catch (IOException e) {
                           throw new UncheckedIOException(e);
                       }
                    });
        }
    }

    private static void deleteDirectoryRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }

        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            LOGGER.warn("Failed to delete file: {}", path, e);
                        }
                    });
        }
    }

    private void executeCommand(String dir, String... command) throws ISchedulerException {
        try {
            String fullCommand = String.join(" ", command);
            LOGGER.info("Executing command {} in directory {}", fullCommand, dir);

            ProcessBuilder pb = new ProcessBuilder(command);
            if(dir != null) {
                pb.directory(new File(dir));
            }

            pb.redirectErrorStream(true);
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[Terraform out] " + line);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("Command failed with exit code " + exitCode + ": " + fullCommand);
            }
        } catch (IOException e) {
            LOGGER.error("Real IOException message: {}", e.getMessage(), e);
            throw new ISchedulerException("I/O error executing: " + String.join(" ", command) + " → " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Interrupted", e);
        }
    }

    private void captureTerraformOutputs(String workDir, String terraformBin) throws ISchedulerException {
        LOGGER.info("Capturing Terraform outputs in directory {}", workDir);

        try{
            ProcessBuilder pb = new ProcessBuilder(terraformBin, "output", "-json");
            if(workDir != null) {
                pb.directory(new File(workDir));
            }

            pb.redirectErrorStream(true);
            Process process = pb.start();

            StringBuilder jsonOutput = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    jsonOutput.append(line);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("Command failed with exit code " + exitCode + ": ");
            }

            String json = jsonOutput.toString().trim();
            if(json.isEmpty()) {
                throw new ISchedulerException("Terraform output was not captured: " + json);
            }

            var root = parseJson(json);

            terraformOutputs.put("subnet_id", getOutputValue(root, "subnet_id"));
            terraformOutputs.put("sg_id", getOutputValue(root, "sg_id"));
            terraformOutputs.put("vpc_id", getOutputValue(root, "vpc_id"));
            terraformOutputs.put("iam_role_arn", getOutputValue(root, "iam_role_arn"));
            terraformOutputs.put("jobs_bucket_name", getOutputValue(root, "jobs_bucket_name"));

            // AMI, Bucket...

            //terraformOutputs.forEach((llave, valor) -> System.out.println(llave + " : " + valor));

        } catch (Exception e){
            LOGGER.error("Failed to capture Terraform outputs in directory {}", workDir, e);
            throw new ISchedulerException("Failed to capture Terraform outputs in directory", e);
        }

    }

    private String getOutputValue(JsonNode root, String outputName) {
        JsonNode node = root.path(outputName).path("value");
        if (node.isMissingNode() || node.isNull()) {
            LOGGER.warn("Output '{}' no encontrado o nulo", outputName);
            return null;
        }
        return node.asText();
    }

    private JsonNode parseJson(String json) throws ISchedulerException {
        try {
            var mapper = new ObjectMapper();
            return mapper.readTree(json);
        } catch (IOException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    private String createEC2Instance(String instanceName, String userDataScript, String amiId, String subnet, String sgId, String iam) throws ISchedulerException {
        try {
            Ec2Client ec2 = Ec2Client.builder()
                    .endpointOverride(URI.create("http://localhost:4566"))
                    .region(Region.US_WEST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                    .build();

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId("ami-df570af1")
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(1)
                    .minCount(1)
                    .subnetId(subnet)
                    .securityGroupIds(sgId)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .arn(iam)
                            .build())
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

    private S3Client getClient() {
        return S3Client.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();

    }

    private String uploadToS3(String bucket, String jobId, String fileName, byte[] data) throws ISchedulerException {
        if (data == null || data.length == 0) {
            throw new ISchedulerException("Data is empty to upload to S3");
        }
        if(fileName == null || fileName.trim().isEmpty()) {
            throw new ISchedulerException("File name is empty to upload to S3");
        }
        
        String key = "jobs/" + jobId + "/" + fileName;
        S3Client s3Client = getClient();
        try{
            PutObjectRequest put = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            s3Client.putObject(put, RequestBody.fromBytes(data));
            return key;

        }catch (Exception e){
            LOGGER.error("Failed to upload To S3", e);
            throw new ISchedulerException("Failed to upload To S3", e);
        }
    }

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {

        String jobId = ISchedulerUtils.genId().substring(0, 8);
        String finalJobName = name.replace("/", "-") + "-" + jobId;

        LOGGER.info("Creating job with name {} and id {}", finalJobName, jobId);

        if(terraformOutputs.isEmpty()) {
            throw new ISchedulerException("Terraform output was not captured: " + finalJobName);
        }

        String subnet = terraformOutputs.get("subnet_id");
        String sg = terraformOutputs.get("sg_id");
        String iamRoleArn = terraformOutputs.get("iam_role_arn");
        String bucket = terraformOutputs.get("jobs_bucket_name");

        if(subnet == null || sg == null || iamRoleArn == null || bucket == null) {
            throw new ISchedulerException("Terraform outputs not found");
        }

        try {
            // Aquí llamaremos al método que creará la instancia en AWS
            /*String instanceId = runEc2Instance(finalJobName, driver);*/

            // Por ahora retornamos el ID para que RunJob sea feliz
            return finalJobName;
        } catch (Exception e) {
            e.printStackTrace(); // <--- AÑADE ESTO para ver el error real en la consola
            LOGGER.error("Error al interactuar con el SDK de AWS", e);
            throw new ISchedulerException("No se pudo levantar la instancia en LocalStack: " + e.getMessage());

        }
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        return null;
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        return null;
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        return null;
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        return null;
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException {
        return null;
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
       return null;
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            // Si esto responde sin error, la conexión con LocalStack está viva
            //ec2Client.describeAvailabilityZones();
        } catch (Exception ex) {
            throw new ISchedulerException("LocalStack no responde: " + ex.getMessage(), ex);
        }
    }


}




