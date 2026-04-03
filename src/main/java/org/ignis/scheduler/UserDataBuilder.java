package org.ignis.scheduler;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class UserDataBuilder {

    private static final String TEMPLATE_RESOURCE_PATH = "scripts/userdata.sh";
    private static final String EXECUTOR_TEMPLATE_PATH = "scripts/userdata-executor.sh";

    public String buildUserData(String region, String jobName, String jobId, String bucket, String bundleKey, String image, String command, String subnet, String sg, String instanceType, String ami, String iamInstanceProfile) throws ISchedulerException{
        String template = loadTemplate(TEMPLATE_RESOURCE_PATH);

        Map<String, String> vars = new HashMap<>();
        vars.put("JOB_NAME", shellEscapeSingleQuotes(jobName));
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("CMD", shellEscapeSingleQuotes(command));
        vars.put("REGION", shellEscapeSingleQuotes(region));
        vars.put("SUBNET_ID", shellEscapeSingleQuotes(subnet));
        vars.put("SG_ID", shellEscapeSingleQuotes(sg));
        vars.put("INSTANCE_TYPE", shellEscapeSingleQuotes(instanceType));
        vars.put("AMI",  shellEscapeSingleQuotes(ami));

        return renderTemplate(template, vars);
    }

    public String buildExecutorUserData(String region, String jobId, String bucket, String bundleKey, String image, String containerName, String executorCmd, Map<String, String> executorEnv) throws ISchedulerException{

        String template = loadTemplate(EXECUTOR_TEMPLATE_PATH);

        Map<String, String> vars = new HashMap<>();
        vars.put("REGION", shellEscapeSingleQuotes(region));
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("CONTAINER_NAME", shellEscapeSingleQuotes(containerName));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("EXECUTOR_CMD", shellEscapeSingleQuotes(executorCmd));
        vars.put("EXECUTOR_ENV_B64", buildEnvBase64(executorEnv));

        return renderTemplate(template, vars);
    }

    private String loadTemplate(String resourcePath) throws ISchedulerException {
        try(InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)){
            if (is == null) throw new ISchedulerException("Resource not found: " + resourcePath);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e){
            throw new ISchedulerException("Failed to load resource: " + resourcePath, e);
        }
    }

    private String renderTemplate(String template, Map<String, String> vars){
        String result = template;
        for(var e: vars.entrySet()){
            result = result.replace("{{" + e.getKey() + "}}", e.getValue());
        }
        return result;
    }

    private static String shellEscapeSingleQuotes(String s) {
        if (s == null) return "";
        return s.replace("'", "'\"'\"'");
    }

    private String buildEnvBase64(Map<String, String> env) {
        if (env == null || env.isEmpty()) return "";

        StringBuilder sb = new StringBuilder();
        for (var e : env.entrySet()) {
            if (e.getKey() == null || e.getKey().isBlank()) continue;
            String value = e.getValue() == null ? "" : e.getValue();
            sb.append(e.getKey()).append("=").append(value).append("\n");
        }

        return Base64.getEncoder().encodeToString(
                sb.toString().getBytes(StandardCharsets.UTF_8)
        );
    }
}
