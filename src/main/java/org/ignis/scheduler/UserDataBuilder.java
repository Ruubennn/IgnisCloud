package org.ignis.scheduler;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class UserDataBuilder {

    private static final String DRIVER_TEMPLATE_PATH = "scripts/userdata.sh";
    private static final String EXECUTOR_TEMPLATE_PATH = "scripts/userdata-executor.sh";

    public String buildUserData(String region, String jobName, String jobId, String bucket, String bundleKey, String subnetId, String sgId, String image, String command, String iamInstanceProfile) throws ISchedulerException{
        String template = loadTemplate(DRIVER_TEMPLATE_PATH);

        Map<String, String> vars = new HashMap<>();
        vars.put("JOB_NAME", shellEscapeSingleQuotes(jobName));
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("CMD", shellEscapeSingleQuotes(command));
        vars.put("REGION", region);
        vars.put("SUBNET_ID", shellEscapeSingleQuotes(subnetId));
        vars.put("SG_ID", shellEscapeSingleQuotes(sgId));
        vars.put("IAM_INSTANCE_PROFILE",  iamInstanceProfile);

        return renderTemplate(template, vars);
    }

    public String buildExecutorUserData(String region, String jobId, String containerName, String bucket, String bundleKey, String image) throws ISchedulerException{
        String template = loadTemplate(EXECUTOR_TEMPLATE_PATH);

        Map<String, String> vars = new HashMap<>();
        vars.put("REGION", region);
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("CONTAINER_NAME", shellEscapeSingleQuotes(containerName));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));

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
}
