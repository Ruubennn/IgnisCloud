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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.ec2.model.ResourceType;

import java.net.URI; // Necesario para LocalStack
import java.util.stream.Stream;

public class Cloud implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final String TF_BIN_PROP = "ignis.terraform.bin";
    private static final String TF_RESOURCE_DIR = "terraform";
    private static boolean CLEANUP_WORKDIR = true;
    private Map<String, String> terraformOutputs = new HashMap<>();

    /*private static final String SUBNET_ID = "subnet-97e34da82877256a3";
    private static final String SECURITY_GROUP_ID = "sg-1800963b8c7b84d1f";
    private static final String AMI_ID = "ami-df570af1";*/

    /*private final String url;*/
    //private final String subnetId;
    //private final String securityGroupId;
    /*private final Ec2Client ec2Client;*/

    /* Esto estaba en el constructor */
      /*this.url = url;
        this.ec2Client = Ec2Client.builder()
                .endpointOverride(URI.create(url))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();

        System.out.println("\n*****************************************");
        System.out.println("¡CONECTADO A LOCALSTACK!");
        System.out.println("Subnet: " + SUBNET_ID);
        System.out.println("SG: " + SECURITY_GROUP_ID);
        System.out.println("*****************************************\n");*/

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

            /*Path tfStateDir = workDir.resolve(".terraform");
            if (Files.exists(tfStateDir)) {
                LOGGER.info("Cleaning up temporary directory: {}", tfStateDir.toString());
                deleteDirectoryRecursively(tfStateDir);
            }

            Path lockFile = workDir.resolve(".terraform.lock.hcl");
            if (Files.exists(lockFile)) {
                LOGGER.info("Removing previous lock file");
                Files.delete(lockFile);
            }*/

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

            // AMI, Bucket...

            terraformOutputs.forEach((llave, valor) -> System.out.println(llave + " : " + valor));

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


    /*private String runEc2Instance(String jobName, IClusterRequest request) throws ISchedulerException {
        try {
            // 1. Preparamos la etiqueta con el nombre (equivalente al bloque 'tags' de tu Terraform)
            Tag nameTag = Tag.builder()
                    .key("Name")
                    .value(jobName)
                    .build();

            TagSpecification tagSpecification = TagSpecification.builder()
                    .resourceType(ResourceType.INSTANCE)
                    .tags(nameTag)
                    .build();

            // 2. Creamos la petición de lanzamiento (equivalente a tu resource "aws_instance")
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(AMI_ID)
                    // Usamos el Enum correspondiente en lugar de un String a machete
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(1)
                    .minCount(1)
                    .subnetId(SUBNET_ID)
                    .securityGroupIds(SECURITY_GROUP_ID)
                    .tagSpecifications(tagSpecification)
                    .build();

            // 3. Ejecutamos la orden en LocalStack
            RunInstancesResponse response = ec2Client.runInstances(runRequest);

            // 4. Obtenemos el ID de la instancia creada
            String instanceId = response.instances().get(0).instanceId();
            System.out.println("[AWS-SDK] Instancia desplegada con éxito. ID: " + instanceId);

            return instanceId;

        } catch (Exception e) {
            LOGGER.error("Error al interactuar con el SDK de AWS", e);
            throw new ISchedulerException("No se pudo levantar la instancia en LocalStack: " + e.getMessage());
        }
    }*/

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {
        // 1. Generamos el ID del Job como hace Docker.java
        String jobId = ISchedulerUtils.genId().substring(0, 8);
        String finalJobName = name.replace("/", "-") + "-" + jobId;

        System.out.println("[CLOUD] Lanzando instancia para el Driver: " + finalJobName);

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




