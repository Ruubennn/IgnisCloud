package org.ignis.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Stream;

public class TerraformManager {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final String TF_BIN_PROP = "ignis.terraform.bin";
    private static final String TF_RESOURCE_DIR = "terraform";
    private static boolean CLEANUP_WORKDIR = true;

    private final String terraformBinary;
    private final Map<String, String> outputs = new HashMap<>();

    public TerraformManager() {
        this.terraformBinary = System.getProperty(TF_BIN_PROP, "terraform");
    }

    public void provision() throws ISchedulerException {
        Path workDir = null;

        try {
            workDir = Files.createTempDirectory("ignis-terraform-");
            LOGGER.debug("Creating temporary directory: {}", workDir.toString());

            copyTerraformResourcesTo(workDir);

            executeTerraform(workDir, "init", "-input=false");
            executeTerraform(workDir, "apply", "-auto-approve", "-input=false");

            captureOutputs(workDir);

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

    public Map<String, String> getOutputs() throws ISchedulerException {
        if (outputs.isEmpty()) {
            throw new ISchedulerException("No outputs available");
        }
        return Collections.unmodifiableMap(outputs);
    }

    public String getOutput(String key, String defaultValue) {
        return outputs.getOrDefault(key, defaultValue);
    }

    public String requireOutput(String key) throws ISchedulerException {
        String value = outputs.get(key);
        if (value == null) {
            throw new ISchedulerException("Output required not found: " + key);
        }
        return value;
    }

    private void copyTerraformResourcesTo(Path destination) throws ISchedulerException, IOException {
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

    private void executeTerraform(Path workDir, String... args) throws ISchedulerException {
        List<String> command = new ArrayList<>();
        command.add(terraformBinary);
        command.addAll(Arrays.asList(args));

        LOGGER.info("Executing command {}", String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(workDir.toFile());
        pb.redirectErrorStream(true);

        try{
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[Terraform out] " + line);
                }
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("Command failed with code " + exitCode);
            }
        } catch (IOException e) {
            LOGGER.error("Real IOException message: {}", e.getMessage(), e);
            throw new ISchedulerException("I/O error executing: " + String.join(" ", command) + " → " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Interrupted", e);
        }
    }

    private void captureOutputs(Path workDir) throws ISchedulerException {
        LOGGER.info("Capturing Terraform outputs in directory {}", workDir);

        ProcessBuilder pb = new ProcessBuilder(terraformBinary, "output", "-json");
        pb.directory(workDir.toFile());
        pb.redirectErrorStream(true);

        try {
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

            outputs.put("subnet_id", getOutputValue(root, "subnet_id"));
            outputs.put("sg_id", getOutputValue(root, "sg_id"));
            outputs.put("vpc_id", getOutputValue(root, "vpc_id"));
            outputs.put("iam_role_arn", getOutputValue(root, "iam_role_arn"));
            outputs.put("jobs_bucket_name", getOutputValue(root, "jobs_bucket_name"));

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

    private static void deleteDirectoryRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;

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
}
