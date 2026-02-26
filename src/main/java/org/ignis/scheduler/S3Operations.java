package org.ignis.scheduler;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

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
}
