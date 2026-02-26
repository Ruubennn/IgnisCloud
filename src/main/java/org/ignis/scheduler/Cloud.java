package org.ignis.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ignis.scheduler.IScheduler;
import org.ignis.scheduler.ISchedulerException;
import org.ignis.scheduler.ISchedulerUtils;
import org.ignis.scheduler.model.*;

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
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI; // Necesario para LocalStack
import java.util.stream.Stream;

public class Cloud implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final String CLOUD_BIND_ROOT = "ignis/dfs";
    private static final String CLOUD_PAYLOAD_DIR = "/ignis/dfs/payload";

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

    private Ec2Client getEc2Client() {
        return Ec2Client.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_WEST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }

    private String createEC2Instance(String instanceName, String userDataScript, String amiId, String subnet, String sgId, String iam) throws ISchedulerException {
        try {
            System.out.println("TEST 1");
            Ec2Client ec2 = getEc2Client();
            System.out.println("TEST 2");
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId("ami-0c55b159cbfafe1f0")
                    .instanceType(InstanceType.T2_MICRO)
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
            System.out.println("TEST 3");
            RunInstancesResponse response = ec2.runInstances(runRequest);
            System.out.println("TEST 4");
            String instanceId = response.instances().get(0).instanceId(); // TODO: se pueden lanzar múltiples instancias? Debería permitirlo?
            System.out.println("TEST 5");

            LOGGER.info("Instance launched: {}", instanceId);
            return instanceId;
        }catch (Exception e){
            LOGGER.error("Failed to create EC2 instance", e);
            if (e instanceof software.amazon.awssdk.services.ec2.model.Ec2Exception) {
                Ec2Exception ex = (Ec2Exception) e;
                System.err.println("AWS Error Code    : " + ex.awsErrorDetails().errorCode());
                System.err.println("AWS Error Message : " + ex.awsErrorDetails().errorMessage());
                System.err.println("Request ID        : " + ex.requestId());
            }
            System.err.println("Full stack trace:");
            e.printStackTrace();
            throw new ISchedulerException("Failed to create EC2 instance", e);
        }
    }

    /*private S3Client getS3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_WEST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();

    }*/

    private S3Client getS3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_WEST_2)
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
                )
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(true)     // <-- CLAVE para LocalStack
                                .chunkedEncodingEnabled(false)    // <-- evita problemas de chunked
                                .build()
                )
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
        S3Client s3Client = getS3Client();
        try{

            PutObjectRequest put = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            s3Client.putObject(put, RequestBody.fromBytes(data));
            LOGGER.info("File uploaded to S3: {}", key);
            return key;

        }catch (Exception e){
            LOGGER.error("Failed to upload To S3", e);
            throw new ISchedulerException("Failed to upload To S3", e);
        }
    }

    private String stripLeadingSlash(String p) {
        if (p == null) return "";
        return p.startsWith("/") ? p.substring(1) : p;
    }

    private void copyRecursively(Path src, Path dst) throws IOException {
        if(Files.isDirectory(src)){
            Files.createDirectories(dst);
            try(Stream<Path> s =  Files.walk(src)){
                for(Path p: s.toList()){
                    Path rel = src.relativize(p);
                    Path target = dst.resolve(rel.toString());

                    if(Files.isDirectory(p)){
                        Files.createDirectories(target);
                    } else {
                        Files.createDirectories(target.getParent());
                        Files.copy(p, target,  StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        } else {
            Files.createDirectories(dst.getParent());
            Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private byte[] buildBindsBundleTarGz(List<IBindMount> binds) throws ISchedulerException {
        if(binds == null || binds.isEmpty()) {
            throw new ISchedulerException("Binds empty");
        }

        Path tmpDir = null;
        try{
            tmpDir = Files.createTempDirectory("ignis-bundle-");
            Path staging =  tmpDir.resolve("staging");
            Files.createDirectories(staging);

            for(IBindMount b : binds) {
                if(b == null) continue;
                if(b.host() == null || b.container() == null) continue;

                Path hostPath = Paths.get(b.host());
                if(!Files.exists(hostPath)) {
                    continue;
                }

                Path containerPathInTar =  staging.resolve(stripLeadingSlash(b.container()));
                copyRecursively(hostPath, containerPathInTar);

            }

            // tar -czf bundle.tar.gz -C staging .
            Path tarGz =  tmpDir.resolve("bundle.tar.gz");
            ProcessBuilder pb = new ProcessBuilder("tar", "-czf",  tarGz.toString(), "-C", staging.toString(), ".");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            int code = p.waitFor();
            if(code != 0){
                throw new ISchedulerException("Failed to copy Binds bundle TarGz");
            }

            return Files.readAllBytes(tarGz);

        } catch (IOException e) {
            throw new ISchedulerException("Failed to create temporary directory", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Interrupted", e);
        } finally {
            if(tmpDir != null) {
                try {
                    deleteDirectoryRecursively(tmpDir);
                } catch (IOException ignored) {}
            }
        }
    }

    private static String shellEscapeSingleQuotes(String s) {
        return s.replace("'", "'\"'\"'");
    }

    /*private String buildUserData(String jobName, String bucket, String bundleKey, IClusterRequest req) {
        List<String> args = (req != null && req.resources() != null) ? req.resources().args() : List.of();
        String cmd = (args == null || args.isEmpty()) ? "echo 'No args provided' && sleep 3600" : String.join(" ", args);

        // Aplica RO aproximado
        String roChmods = "";
        if (req != null && req.resources() != null && req.resources().binds() != null) {
            roChmods = req.resources().binds().stream()
                    .filter(b -> b != null && b.ro() && b.container() != null && !b.container().isBlank())
                    .map(b -> "chmod -R a-w " + b.container() + " || true")
                    .collect(Collectors.joining("\n"));
        }

        // Ojo: el AMI debe traer curl + tar. Si no, instala (Amazon Linux: yum; Ubuntu: apt).
        // Para LocalStack/entorno controlado, puedes simplificar.
        return """
            #!/bin/bash
            set -euo pipefail

            # (Opcional) instala dependencias según tu AMI:
            if command -v yum >/dev/null 2>&1; then
              yum -y update || true
              yum -y install awscli tar gzip curl || true
            elif command -v apt-get >/dev/null 2>&1; then
              apt-get update -y || true
              apt-get install -y awscli tar gzip curl || true
            fi

            export IGNIS_SCHEDULER_ENV_JOB='%s'

            # instance-id por metadata (en AWS real funciona; en LocalStack puede depender)
            IID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
            export IGNIS_SCHEDULER_ENV_CONTAINER="$IID"

            aws --region us-west-2 s3 cp 's3://%s/%s' /tmp/bundle.tar.gz
            tar -xzf /tmp/bundle.tar.gz -C /

            %s

            exec /bin/bash -lc '%s'
            """.formatted(
                shellEscapeSingleQuotes(jobName),
                shellEscapeSingleQuotes(bucket),
                shellEscapeSingleQuotes(bundleKey),
                roChmods,
                shellEscapeSingleQuotes(cmd)
        );
    }*/

    private String buildUserData(String jobName, String bucket, String bundleKey, String jobId, String image, String cmd) throws ISchedulerException {
        String template = loadResourceAsString("scripts/userdata.sh");

        Map<String, String> vars = new HashMap<>();
        vars.put("JOB_NAME", shellEscapeSingleQuotes(jobName));
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("CMD", shellEscapeSingleQuotes(cmd));
        vars.put("REGION", "us-west-2");

        return renderUserDataTemplate(template, vars);
    }

    private String loadResourceAsString(String resourcePath) throws ISchedulerException {
        try(InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)){
            if (is == null) throw new ISchedulerException("Resource not found: " + resourcePath);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e){
            throw new ISchedulerException("Failed to load resource: " + resourcePath, e);
        }
    }

    private String renderUserDataTemplate(String template, Map<String, String> vars){
        String out = template;
        for(var e: vars.entrySet()){
            out = out.replace("{{" + e.getKey() + "}}", e.getValue());
        }
        return out;
    }

    private Path detectMainScript(List<String> args){
        if(args == null || args.isEmpty()) return null;
        for(String a: args){
            if(a == null) continue;

            Path path = Paths.get(a);
            if(Files.exists(path) && Files.isRegularFile(path)){
                return path.toAbsolutePath().normalize();
            }
        }
        return null;
    }

    private List<IBindMount> buildPayloadBindsFromArgs(IClusterRequest driver){
        Path script = detectMainScript(driver.resources().args());
        if(script == null) return List.of();

        String cloudTarget = CLOUD_PAYLOAD_DIR + "/" + script.getFileName();
        return List.of(new IBindMount(cloudTarget, script.toString(), true));
    }

    private List<String> rewriteArgsToCloud(List<String> args, Path script) {
        if (args == null || script == null) return args;
        String local = script.toString();
        String cloud = CLOUD_PAYLOAD_DIR + "/" + script.getFileName();

        return args.stream()
                .map(a -> a != null && a.equals(local) ? cloud : a)
                .toList();
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

            List<IBindMount> binds = buildPayloadBindsFromArgs(driver);
            byte[] bundle = buildBindsBundleTarGz(binds);
            String bundleKey = uploadToS3(bucket, jobId, "bundle.tar.gz", bundle);

            Path script = detectMainScript(driver.resources().args());
            String cloudScriptPath = "/ignis/dfs/payload/" + script.getFileName();
            String cmd = "python3 " + cloudScriptPath;

            /*String userData = buildUserData(finalJobName, bucket, bundleKey, jobId,
                    driver.resources().image(), cmd);*/
            // TODO: despliegue en aws
            String userData = buildUserData(finalJobName, bucket, bundleKey, jobId, "python:3.11-slim", cmd);


            //String userData = buildUserData(finalJobName, bucket, bundleKey, jobId, driver.resources().image(), driver.resources().args());
            String instanceId = createEC2Instance(finalJobName + "-driver", userData, "ami-df570af1", subnet, sg, iamRoleArn);

            return finalJobName;

        } catch (Exception e) {
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




