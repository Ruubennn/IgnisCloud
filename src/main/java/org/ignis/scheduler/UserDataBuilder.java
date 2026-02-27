package org.ignis.scheduler;

import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class UserDataBuilder {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final String TEMPLATE_RESOURCE_PATH = "scripts/userdata.sh";
    private static final String DEFAULT_IMAGE = "python:3.11-slim";

    public String buildUserData(String jobName, String jobId, String bucket, String bundleKey, String image, String command) throws ISchedulerException{
        String template = loadTemplate();

        Map<String, String> vars = new HashMap<>();
        vars.put("JOB_NAME", shellEscapeSingleQuotes(jobName));
        vars.put("JOB_ID", shellEscapeSingleQuotes(jobId));
        vars.put("BUCKET", shellEscapeSingleQuotes(bucket));
        vars.put("BUNDLE_KEY", shellEscapeSingleQuotes(bundleKey));
        vars.put("IMAGE", shellEscapeSingleQuotes(image));
        vars.put("CMD", shellEscapeSingleQuotes(command));
        vars.put("REGION", "us-west-2");

        return renderTemplate(template, vars);
    }

    private String loadTemplate() throws ISchedulerException {
        try(InputStream is = getClass().getClassLoader().getResourceAsStream(TEMPLATE_RESOURCE_PATH)){
            if (is == null) throw new ISchedulerException("Resource not found: " + TEMPLATE_RESOURCE_PATH);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e){
            throw new ISchedulerException("Failed to load resource: " + TEMPLATE_RESOURCE_PATH, e);
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
        return s.replace("'", "'\"'\"'");
    }
}
