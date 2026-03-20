package org.ignis.scheduler;

import org.ignis.scheduler.model.IBindMount;
import org.ignis.scheduler.model.IClusterRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PayloadResolver {

    private static final String CLOUD_PAYLOAD_DIR = "/ignis/dfs/payload";

    // Reference: [38], [39]
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

    public List<IBindMount> buildPayloadBindsFromArgs(IClusterRequest driver) {
        Path script = detectMainScript(driver.resources().args());
        if (script == null) {
            return List.of();
        }

        // ── CAMBIO IMPORTANTE ────────────────────────────────
        Path scriptDir = script.getParent();                    // ← directorio padre
        if (scriptDir == null || !Files.isDirectory(scriptDir)) {
            // fallback: solo el script (caso raro)
            String cloudTarget = CLOUD_PAYLOAD_DIR + "/" + script.getFileName();
            return List.of(new IBindMount(cloudTarget, script.toString(), true));
        }

        // Subimos TODO el directorio donde está el script
        // → /ignis/dfs/payload/ contendrá test.py, text.txt, test_2mb.txt, etc.
        return List.of(
                new IBindMount(CLOUD_PAYLOAD_DIR, scriptDir.toAbsolutePath().toString(), true)
        );
    }

    /*public List<IBindMount> buildPayloadBindsFromArgs(IClusterRequest driver){
        Path script = detectMainScript(driver.resources().args());
        if(script == null) return List.of();

        String cloudTarget = CLOUD_PAYLOAD_DIR + "/" + script.getFileName();
        return List.of(new IBindMount(cloudTarget, script.toString(), true));
    }*/

    public String resolveCloudScriptPath(IClusterRequest driver) {
        Path script = detectMainScript(driver.resources().args());
        if (script == null) {
            return null;
        }
        return CLOUD_PAYLOAD_DIR + "/" + script.getFileName();
    }

    public String resolveCommand(IClusterRequest driver) {
        List<String> args = driver.resources().args();
        if (args == null || args.isEmpty()) return "";

        Path mainLocalScript = detectMainScript(args);
        String cloudScriptPath = (mainLocalScript != null) ? resolveCloudScriptPath(driver) : null;

        int startIndex = 0;
        if ("ignis-job".equals(args.get(0))) {
            startIndex = 1; // TODO: con el prueba.sh para que IMAGE_CMD = python3 /ignis/dfs/payload/test.py es 2
        }

        List<String> command = new ArrayList<>();
        for (int i = startIndex; i < args.size(); i++) {
            String arg = args.get(i);
            if (arg == null) continue;

            if (mainLocalScript != null) {
                Path candidate = Paths.get(arg);
                try {
                    if (Files.exists(candidate) &&
                            Files.isRegularFile(candidate) &&
                            candidate.toAbsolutePath().normalize().equals(mainLocalScript)) {
                        command.add(cloudScriptPath);
                        continue;
                    }
                } catch (Exception ignored) {}
            }

            command.add(arg);
        }

        return String.join(" ", command);
    }

    public String resolveImage(IClusterRequest driver) {
        String image = driver.resources().image();
        if(image == null || image.contains("ignishpc/ignishpc")){
            List<String> args = driver.resources().args();
            if(args.size() > 1 && args.get(1).contains("/")){
                image = args.get(1);
            }
        }
        return image;
    }
}
