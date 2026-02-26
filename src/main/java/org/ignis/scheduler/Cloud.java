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

    private final TerraformManager terraformManager;
    private final AwsFactory awsFactory;
    private final EC2Operations ec2;
    private final S3Operations s3;
    /*private final UserDataBuilder userDataBuilder;
    private final BundleCreator bundleCreator;*/


    public Cloud(String url) throws ISchedulerException{

        System.out.println("\n*****************************************");
        System.out.println("URL: " + url);
        System.out.println("*****************************************\n");

        LOGGER.info("Initializing Cloud scheduler at: {}", url);

        this.terraformManager = new TerraformManager();
        this.awsFactory = new AwsFactory();
        this.ec2 = new EC2Operations(awsFactory);
        this.s3 = new S3Operations(awsFactory);

        this.terraformManager.provision();

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

    private Ec2Client getEc2Client() {
        return awsFactory.createEc2Client();
    }

    private S3Client getS3Client() {
        return awsFactory.createS3Client();
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

        String subnet = terraformManager.requireOutput("subnet_id");
        String sg = terraformManager.requireOutput("sg_id");
        String iamRoleArn = terraformManager.requireOutput("iam_role_arn");
        String bucket = terraformManager.requireOutput("jobs_bucket_name");

        if(subnet == null || sg == null || iamRoleArn == null || bucket == null) {
            throw new ISchedulerException("Terraform outputs not found");
        }

        try {

            List<IBindMount> binds = buildPayloadBindsFromArgs(driver);
            byte[] bundle = buildBindsBundleTarGz(binds);
            String bundleKey = s3.uploadJobBundle(bucket, jobId, bundle);

            Path script = detectMainScript(driver.resources().args());
            String cloudScriptPath = "/ignis/dfs/payload/" + script.getFileName();
            String cmd = "python3 " + cloudScriptPath;

            /*String userData = buildUserData(finalJobName, bucket, bundleKey, jobId,
                    driver.resources().image(), cmd);*/
            // TODO: despliegue en aws
            String userData = buildUserData(finalJobName, bucket, bundleKey, jobId, "python:3.11-slim", cmd);


            //String userData = buildUserData(finalJobName, bucket, bundleKey, jobId, driver.resources().image(), driver.resources().args());
            String instanceId = ec2.createEC2Instance(finalJobName + "-driver", userData, "ami-df570af1", subnet, sg, iamRoleArn);

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