package org.ignis.scheduler;

import org.ignis.scheduler.model.IBindMount;
import org.ignis.scheduler.model.IClusterRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class PayloadResolver {

    private static final String CLOUD_PAYLOAD_DIR = "/ignis/dfs/payload";

    public Path detectMainScript(List<String> args){
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

    public List<IBindMount> buildPayloadBindsFromArgs(IClusterRequest driver){
        Path script = detectMainScript(driver.resources().args());
        if(script == null) return List.of();

        String cloudTarget = CLOUD_PAYLOAD_DIR + "/" + script.getFileName();
        return List.of(new IBindMount(cloudTarget, script.toString(), true));
    }

    public String resolveCloudScriptPath(IClusterRequest driver) {
        Path script = detectMainScript(driver.resources().args());
        if (script == null) {
            return null;
        }
        return CLOUD_PAYLOAD_DIR + "/" + script.getFileName();
    }
}
