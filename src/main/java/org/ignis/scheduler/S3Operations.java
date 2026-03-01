package org.ignis.scheduler;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class S3Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);

    private static final String JOBS_PREFIX = "jobs/";
    private static final String DEFAULT_BUNDLE_FILENAME = "bundle.tar.gz";

    private final AwsFactory awsFactory;

    public S3Operations(AwsFactory awsFactory) {
        this.awsFactory = awsFactory;
    }

    public String uploadJobBundle(String bucket, String jobId, byte[] bundleData)
            throws ISchedulerException {
        return uploadFile(bucket, jobId, DEFAULT_BUNDLE_FILENAME, bundleData);
    }

    // Reference: [27]
    public String uploadFile(String bucket, String jobId, String fileName, byte[] data) throws ISchedulerException {
        validateUploadParams(bucket, jobId, fileName, data);
        String key = buildKey(jobId, fileName);
        S3Client s3Client = awsFactory.createS3Client();
        try{
            PutObjectRequest put = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            s3Client.putObject(put, RequestBody.fromBytes(data));
            LOGGER.info("File uploaded to S3: {}", key);
            return key;
        }catch (Exception e){
            LOGGER.error("Failed to upload To S3", e);
            throw new ISchedulerException("Failed to upload To S3", e);
        }
    }

    private void validateUploadParams(String bucket, String jobId, String filename, byte[] data){
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Data is empty to upload to S3");
        }
        if(filename == null || filename.trim().isEmpty()) {
            throw new IllegalArgumentException("Filename is empty to upload to S3");
        }
        if(bucket == null || bucket.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket is empty to upload to S3");
        }
        if(jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("JobId is empty to upload to S3");
        }
    }

    private String buildKey(String jobId, String fileName){
        String cleanFileName = fileName.trim().replaceAll("^/+", "").replaceAll("/+$", "");
        return JOBS_PREFIX + jobId + "/" + cleanFileName;
    }


    // Reference: [41]
    private List<S3Object> listObjectsInBucket(String bucket, String prefix) throws ISchedulerException {
        if(bucket == null || bucket.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket is empty to list S3 Objects");
        }

        String effectivePrefix = (prefix != null) ? prefix.trim() : "";
        if (effectivePrefix.startsWith("/")) {
            effectivePrefix = effectivePrefix.substring(1);
        }
        if (effectivePrefix.endsWith("/")) {
            effectivePrefix = effectivePrefix.substring(0, effectivePrefix.length() - 1);
        }

        S3Client s3 = this.awsFactory.createS3Client();
        String nextContinuationToken = null;
        List<S3Object> contents = new ArrayList<>();

        try{
            do{
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(effectivePrefix);

                if(nextContinuationToken != null) {
                    requestBuilder.continuationToken(nextContinuationToken);
                }

                ListObjectsV2Response response = s3.listObjectsV2(requestBuilder.build());

                for(S3Object s3Object : response.contents()) {
                    if(!s3Object.key().endsWith("/")) {
                        contents.add(s3Object);
                    }
                }
                nextContinuationToken = response.nextContinuationToken();

            }while(nextContinuationToken != null);
            LOGGER.info("Listing S3 Objects: {}", contents);

            return contents;
        }catch (NoSuchBucketException e) {
            throw new ISchedulerException("Bucket not exists: " + bucket, e);
        } catch (S3Exception e) {
            LOGGER.error("Error listing S3 Objects: {}", e.getMessage());
            throw new ISchedulerException("Error listing S3 Objects: " + e.awsErrorDetails().errorMessage(), e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error listing S3 Objects: {}", e.getMessage());
            throw new ISchedulerException("Unexpected error listing S3 Objects", e);
        }
    }

    private void validateDownloadParams(String bucket, String prefix, String localDir) throws ISchedulerException {
        if(bucket == null || bucket.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket is empty to download S3 Objects");
        }
        if(prefix == null || prefix.trim().isEmpty()) {
            throw new IllegalArgumentException("Prefix is empty to download S3 Objects");
        }
        if(localDir == null || localDir.trim().isEmpty()) {
            throw new IllegalArgumentException("LocalDir is empty to download S3 Objects");
        }
    }

    // Reference: [41]
    private int downloadObjects(String bucket, String prefix, String localDir) throws ISchedulerException {
       validateDownloadParams(bucket, prefix, localDir);
       Path basePath = Paths.get(localDir);
       try{
           Files.createDirectories(basePath);
       } catch (IOException e){
           throw new ISchedulerException("Error creating directory: " + localDir, e);
       }

       List<S3Object> objects = listObjectsInBucket(bucket, prefix);
       int succesCount = 0;
       S3Client s3 = this.awsFactory.createS3Client();

       for(S3Object s3Object : objects) {
           String key =  s3Object.key();

           String relativePath = key.substring(prefix.length());
           if(relativePath.isEmpty()) continue;

           Path targetPath = basePath.resolve(relativePath);

           try{
               Files.createDirectories(targetPath.getParent());

               GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                       .bucket(bucket)
                       .key(key)
                       .build();

               s3.getObject(getObjectRequest, targetPath);
               LOGGER.info("Downloaded S3 Object: {}", targetPath);
               succesCount++;

           } catch (Exception e){
               throw new ISchedulerException("Error downloading", e);
           }
       }
       LOGGER.info("Download S3 Objects: {}", objects);
       return succesCount;
    }

    public void downloadJob(String jobId, String bucket) throws ISchedulerException {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("jobId should not be empty");
        }

        String prefix = "jobs/" + jobId.trim() + "/";
        String configuredDir = System.getenv("IGNIS_DOWNLOAD_DIR");

        String baseDir;
        if (configuredDir != null && !configuredDir.trim().isEmpty()) {
            baseDir = configuredDir.trim();
            LOGGER.debug("Using directory configured as environment variable: {}", baseDir);
        } else {
            baseDir = Paths.get("").toAbsolutePath().toString();
            LOGGER.debug("IGNIS_DOWNLOAD_DIR not found. Using directory: {}", baseDir);
        }

        String localDir = Paths.get(baseDir, "ignis-downloads", jobId).toString();

        try {
            Files.createDirectories(Paths.get(localDir));
            LOGGER.info("Downloading job {} → target: {}", jobId, localDir);
            int count = downloadObjects(bucket, prefix, localDir);
            LOGGER.info("Download completed: {} objects in {}", count, localDir);

        } catch (IOException e) {
            throw new ISchedulerException("The download directory could not be created: " + localDir, e);
        } catch (Exception e) {
            throw new ISchedulerException("Failure downloading objects at job " + jobId, e);
        }
    }

    public void deleteJobObjects(String bucket, String jobId) throws ISchedulerException {
        if (jobId == null || jobId.trim().isEmpty()) {
            throw new IllegalArgumentException("jobId cannot be empty");
        }

        String prefix = JOBS_PREFIX + jobId.trim() + "/";

        List<S3Object> objects = listObjectsInBucket(bucket, prefix);
        if (objects.isEmpty()) {
            LOGGER.info("No objects found to delete for job {}", jobId);
            return;
        }

        S3Client s3 = awsFactory.createS3Client();

        DeleteObjectsRequest.Builder deleteBuilder = DeleteObjectsRequest.builder()
                .bucket(bucket);

        List<ObjectIdentifier> toDelete = objects.stream()
                .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                .toList();

        if (!toDelete.isEmpty()) {
            deleteBuilder.delete(Delete.builder().objects(toDelete).build());

            try {
                s3.deleteObjects(deleteBuilder.build());
                LOGGER.info("Deleted {} objects for job {} in bucket {}",
                        toDelete.size(), jobId, bucket);
            } catch (Exception e) {
                LOGGER.error("Failed to delete objects for job {}", jobId, e);
                throw new ISchedulerException("Failed to delete job objects from S3", e);
            }
        }
    }
}
