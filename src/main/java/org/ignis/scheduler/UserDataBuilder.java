package org.ignis.scheduler;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserDataBuilder {

    private static final String DRIVER_TEMPLATE_PATH = "scripts/userdata.sh";
    private static final String EXECUTOR_TEMPLATE_PATH = "scripts/userdata-executor.sh";

    public String buildUserData(String region, String jobName, String jobId, String bucket, String bundleKey, String image, String command, String subnet, String sg, String instanceType, String ami) throws ISchedulerException{
        String template = loadTemplate(DRIVER_TEMPLATE_PATH);

        Map<String, String> vars = new HashMap<>();
        vars.put("JOB_NAME", shellEscapeSingleQuotes(jobName));
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("CMD", shellEscapeSingleQuotes(command));
        vars.put("REGION", region);
        vars.put("SUBNET_ID", shellEscapeSingleQuotes(subnet));
        vars.put("SG_ID", shellEscapeSingleQuotes(sg));
        vars.put("INSTANCE_TYPE", shellEscapeSingleQuotes(instanceType));
        vars.put("AMI",  shellEscapeSingleQuotes(ami));

        return renderTemplate(template, vars);
    }

    public String buildExecutorUserData(String region, String jobId, String containerName,
                                        String bucket, String bundleKey, String image,
                                        Map<String, String> env, List<String> args)
            throws ISchedulerException {

        String template = loadTemplate(EXECUTOR_TEMPLATE_PATH);

        StringBuilder envFlags = new StringBuilder();
        for (var e : env.entrySet()) {
            String key = e.getKey();
            String value = e.getValue() == null ? "" : e.getValue();

            // Escapado ROBUSTO para estar dentro de comillas dobles en bash
            value = value
                    .replace("\\", "\\\\")   // primero las barras
                    .replace("\"", "\\\"")   // comillas dobles
                    .replace("$", "\\$")     // muy importante
                    .replace("`", "\\`")     // backticks
                    .replace("!", "\\!");    // history expansion

            envFlags.append("-e \"").append(key).append("=")
                    .append(value).append("\" ");
        }

        Map<String, String> vars = new HashMap<>();
        vars.put("REGION", region);
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("CONTAINER_NAME", shellEscapeSingleQuotes(containerName));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("EXECUTOR_ENV", envFlags.toString().trim()); // quitamos espacio final
        vars.put("EXECUTOR_CMD", String.join(" ", args));

        return renderTemplate(template, vars);
    }

    /*public String buildExecutorUserData(String region, String jobId, String containerName, String bucket, String bundleKey, String image, Map<String, String> env, List<String> args) throws ISchedulerException{
        String template = loadTemplate(EXECUTOR_TEMPLATE_PATH);

        //StringBuilder envFlags = new StringBuilder();
        //for (var e : env.entrySet()) {
          //  envFlags.append("  -e ").append(e.getKey()).append("='")
         //           .append(shellEscapeSingleQuotes(e.getValue())).append("' \\\n");
        //}

        StringBuilder envFlags = new StringBuilder();
        for (var e : env.entrySet()) {
            String value = e.getValue() == null ? "" : e.getValue();
            // Escapar comillas dobles dentro del valor
            value = value.replace("\\", "\\\\").replace("\"", "\\\"");
            envFlags.append("-e \"").append(e.getKey()).append("=")
                    .append(value).append("\" ");
        }

        Map<String, String> vars = new HashMap<>();
        vars.put("REGION", region);
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("CONTAINER_NAME", shellEscapeSingleQuotes(containerName));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("EXECUTOR_ENV", envFlags.toString());
        vars.put("EXECUTOR_CMD", String.join(" ", args));

        return renderTemplate(template, vars);
    }*/

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
