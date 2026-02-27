package org.ignis.scheduler;

import org.ignis.scheduler.model.IBindMount;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class BundleCreator {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final String STAGING_DIR_NAME = "staging";
    private static final String TAR_FILENAME = "bundle.tar.gz";

    // Reference: [32]
    public byte[] createBundleTarGz(List<IBindMount> binds) throws ISchedulerException {
        if(binds == null || binds.isEmpty()) {
            throw new ISchedulerException("Binds empty");
        }

        Path tmpDir = null;
        try{
            tmpDir = Files.createTempDirectory("ignis-bundle-");
            Path staging =  tmpDir.resolve(STAGING_DIR_NAME);
            Files.createDirectories(staging);

            copyBindsToStaging(binds, staging);
            Path tarGzPath = createTarGz(tmpDir, staging);

            return Files.readAllBytes(tarGzPath);

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

    // Reference: [10], [11], [31]
    // Why tar.gz?  [36], [37]
    private Path createTarGz(Path tmpDir, Path staging) throws IOException, InterruptedException {
        Path tarGz =  tmpDir.resolve(TAR_FILENAME);
        ProcessBuilder pb = new ProcessBuilder("tar", "-czf",  tarGz.toString(), "-C", staging.toString(), ".");
        pb.redirectErrorStream(true);
        Process p = pb.start();

        int code = p.waitFor();
        if(code != 0){
            throw new IOException("Failed to copy Binds bundle TarGz");
        }
        return tarGz;
    }

    private void copyBindsToStaging(List<IBindMount> binds, Path stagingDir) throws IOException {
        for (IBindMount bind : binds) {
            if (bind == null || bind.host() == null || bind.container() == null) {
                continue;
            }

            Path hostPath = Paths.get(bind.host());
            if (!Files.exists(hostPath)) {
                LOGGER.warn("Ruta host no existe, se ignora: {}", hostPath);
                continue;
            }

            String containerRel = stripLeadingSlash(bind.container());
            Path targetPath = stagingDir.resolve(containerRel);

            LOGGER.debug("Copiando {} → {}", hostPath, targetPath);

            copyRecursively(hostPath, targetPath);
        }
    }

    // Reference: [30]
    private String stripLeadingSlash(String p) {
        if (p == null) return "";
        return p.startsWith("/") ? p.substring(1) : p;
    }

    // Reference: [33], [34], [35]
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

    // Reference: [16]
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
