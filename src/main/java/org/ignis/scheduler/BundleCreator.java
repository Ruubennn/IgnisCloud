package org.ignis.scheduler;

import org.ignis.scheduler.model.IBindMount;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class BundleCreator {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BundleCreator.class);

    private static final String STAGING_DIR_NAME = "staging";
    private static final String TAR_FILENAME = "bundle.tar.gz";

    private static final long LARGE_FILE_THRESHOLD_BYTES = Long.parseLong(
            System.getenv().getOrDefault("IGNIS_LARGE_FILE_THRESHOLD_MB", "10") // Default: 10 MB
    ) * 1024 * 1024;
    private static final int MAX_FILES = 5000;
    private static final Set<String> EXCLUDED = Set.of(".git", "__pycache__", ".DS_Store", ".vscode", ".idea", "node_modules", "venv", "env");

    public BundleResult createBundleTarGzHybrid(List<IBindMount> binds, String bucket, String jobId, S3Operations s3) throws ISchedulerException {
        if(binds == null || binds.isEmpty()) {
            throw new ISchedulerException("No payload or jar libraries were detected to bundle.");
        }

        Path tmpDir = null;
        List<LargeFile> largeFiles = new ArrayList<>();
        int fileCount = 0;

        try{
            tmpDir = Files.createTempDirectory("ignis-bundle-");
            Path staging =  tmpDir.resolve(STAGING_DIR_NAME);
            Files.createDirectories(staging);

            fileCount = copyBindsToStagingHybrid(binds, staging, bucket, jobId, s3, largeFiles);

            if (fileCount > MAX_FILES) {
                throw new ISchedulerException("Too many files in payload (" + fileCount + "). Max allowed: " + MAX_FILES);
            }

            Path tarGzPath = createTarGz(tmpDir, staging);
            byte[] tarData = Files.readAllBytes(tarGzPath);

            LOGGER.info("Bundle created: {} small files + {} large files (uploaded directly to S3)",
                    fileCount - largeFiles.size(), largeFiles.size());

            return new BundleResult(tarData, largeFiles);

        } catch (IOException e) {
            throw new ISchedulerException("Failed to create bundle", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Interrupted", e);
        } finally {
            if (tmpDir != null) {
                try { deleteDirectoryRecursively(tmpDir); } catch (IOException ignored) {}
            }
        }
    }

    private int copyBindsToStagingHybrid(List<IBindMount> binds, Path stagingDir, String bucket, String jobId, S3Operations s3, List<LargeFile> largeFiles) throws IOException, ISchedulerException {
        int count = 0;

        for (IBindMount bind : binds) {
            if (bind == null || bind.host() == null || bind.container() == null) continue;

            Path hostPath = Paths.get(bind.host());
            if (!Files.exists(hostPath)) continue;

            String containerBase = stripLeadingSlash(bind.container());

            try (Stream<Path> walk = Files.walk(hostPath)) {
                for (Path p : walk.toList()) {
                    if (shouldExclude(p)) continue;

                    if (Files.isRegularFile(p)) {
                        count++;
                        String rel = hostPath.relativize(p).toString();
                        String targetRel = containerBase.isEmpty() ? rel : containerBase + "/" + rel;

                        long size = Files.size(p);
                        if (size > LARGE_FILE_THRESHOLD_BYTES) {
                            // ¡SUBIDA DIRECTA A S3!
                            String s3Key = s3.uploadLargeFile(bucket, jobId, targetRel, p);
                            largeFiles.add(new LargeFile(targetRel, s3Key));
                            continue; // NO va al tar.gz
                        }

                        // Fichero pequeño → al staging
                        Path targetPath = stagingDir.resolve(targetRel);
                        copyRecursively(p, targetPath); // reutilizamos tu método
                    } else if (Files.isDirectory(p)) {
                        // crear carpetas vacías si hace falta
                        Path targetPath = stagingDir.resolve(hostPath.relativize(p).toString());
                        Files.createDirectories(targetPath);
                    }
                }
            } catch (ISchedulerException e) {
                throw e;
            }
        }

        return count;
    }

    private boolean shouldExclude(Path path) {
        String name = path.getFileName().toString().toLowerCase();
        return EXCLUDED.contains(name) || name.endsWith(".pyc") || name.endsWith(".log") || name.startsWith(".");
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
