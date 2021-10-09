/*********************************************************************************
 * Copyright (c)2020 CEC Health
 * FILE: CS3ServiceImpl
 * 版本      DATE             BY               REMARKS
 * ----  -----------  ---------------  ------------------------------------------
 * 1.0   2020-06-08        xiwu
 ********************************************************************************/
package org.aaa.ceph.service;


import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.amazonaws.util.StringUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import okhttp3.HttpUrl;
import org.aaa.ceph.constant.TransferManagerConf;
import org.aaa.ceph.exception.CephException;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * common simple storage service Service Implementation
 *
 * @Author: wuxi
 * @Date: 2019/3/13
 */
public class CS3ServiceImpl implements CS3Service {

  private final AmazonS3 amazonS3;

  /**
   * Create a S3 operation implementation
   *
   * @param accessKey Access key of the user who have proper administrative capabilities.
   * @param secretKey Secret key of the user who have proper administrative capabilities.
   * @param endpoint  Radosgw user  API endpoint, e.g., http://127.0.0.1:80
   */
  public CS3ServiceImpl(String accessKey, String secretKey, String endpoint) {
    validEndpoint(endpoint);
    AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.HTTP);
    amazonS3 = AmazonS3ClientBuilder.standard()
            .withClientConfiguration(clientConfig)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, ""))
            .withPathStyleAccessEnabled(true)
            .build();

  }


  private static void validEndpoint(String endpoint) {
    if (HttpUrl.parse(endpoint) == null) {
      throw new IllegalArgumentException("endpoint is invalid");
    }
  }

  @Override
  public List<Bucket> listBuckets() {
    List<Bucket> buckets = new ArrayList<>();
    try {
      buckets = amazonS3.listBuckets();
      for (Bucket bucket : buckets) {
        System.out.println(bucket.getName() + "\t" + StringUtils.fromDate(bucket.getCreationDate()));
      }
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
    return buckets;
  }

  @Override
  public Bucket createBucket(String bucketName) {
    Bucket bucket = null;
    try {
      //List<Bucket> buckets = amazonS3.listBuckets();
      // bucket existed
      if (!amazonS3.doesBucketExistV2(bucketName)) {
        bucket = amazonS3.createBucket(bucketName);
      }
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
    return bucket;
  }

  @Override
  public void removeBucket(String bucketName) {
    try {
      if (amazonS3.doesBucketExistV2(bucketName)) {
        amazonS3.deleteBucket(bucketName);
      }
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
    return;
  }

  @Override
  public ObjectListing listObjects(String bucketName) {
    ObjectListing objects;
    try {
      objects = amazonS3.listObjects(bucketName);
      for (S3ObjectSummary object : objects.getObjectSummaries()) {
        System.out.println(object.getKey() + "\t" + object.getSize() + "\t" + StringUtils.fromDate(object.getLastModified()));
      }
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
    return objects;
  }

  @Override
  public S3ObjectSummary createObject(String bucketName, String objectKey, File file) {
    S3ObjectSummary newObject = new S3ObjectSummary();
    if (!file.isFile()) {
      throw new CephException("Upload file is wrong");
    }
    try {
      amazonS3.putObject(bucketName, objectKey, file);
      ObjectListing objects = amazonS3.listObjects(bucketName);
      for (S3ObjectSummary object : objects.getObjectSummaries()) {
        if (object.getKey().equals(objectKey)) {
          newObject = object;
          break;
        }
      }
    } catch (AmazonS3Exception e) {
      throw new CephException(e.getMessage());
    }
    return newObject;
  }

  @Override
  public S3ObjectSummary createObject(String bucketName, String objectKey, InputStream input) {
    S3ObjectSummary newObject = new S3ObjectSummary();
    try {
      amazonS3.putObject(bucketName, objectKey, input, new ObjectMetadata());
      ObjectListing objects = amazonS3.listObjects(bucketName);
      for (S3ObjectSummary object : objects.getObjectSummaries()) {
        if (object.getKey().equals(objectKey)) {
          newObject = object;
          break;
        }
      }
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
    return newObject;
  }

  @Override
  public S3Object getObject(String bucketName, String objectKey) {
    S3Object object;
    try {
      object = amazonS3.getObject(bucketName, objectKey);
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
    return object;
  }

  @Override
  public void getObject(String bucketName, String objectKey, File file) {
    try {
      GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
      amazonS3.getObject(request, file);
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
  }

  @Override
  public void removeObject(String bucketName, String objectKey) {
    try {
      if (amazonS3.doesObjectExist(bucketName, objectKey)) {
        amazonS3.deleteObject(bucketName, objectKey);
      }
    } catch (AmazonS3Exception e) {
      e.printStackTrace();
      throw new CephException(e.getMessage());
    }
  }

  @Override
  public void highLevelPartUpload(String bucketName, String objectKey, File file) {
    try {
      TransferManager tm = TransferManagerBuilder.standard()
              .withS3Client(amazonS3)
              .build();

      // TransferManager processes all transfers asynchronously,
      // so this call returns immediately.
      PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, file);
      // set the listener
      request.setGeneralProgressListener(new ProgressListener() {
        Long totalSize = 0L;

        @Override
        public void progressChanged(ProgressEvent progressEvent) {
          Long transferred = progressEvent.getBytesTransferred();
          totalSize = totalSize + transferred;
          System.out.println("total transferred bytes: " + totalSize);
        }
      });
      Upload upload = tm.upload(request);
      System.out.println("Object upload started");
      // Optionally, wait for the upload to finish before continuing.
      upload.waitForCompletion();
      System.out.println("Object upload complete");
    } catch (InterruptedException | SdkClientException e) {
      // The call was transmitted successfully, but Amazon S3 couldn't process
      // it, so it returned an error response.
      e.printStackTrace();
    }
  }

  @Override
  public void highLevelPartUpload(String bucketName, String objectKey, InputStream inputStream) {
    try {
      TransferManager tm = TransferManagerBuilder.standard()
              .withS3Client(amazonS3)
              .build();

      // TransferManager processes all transfers asynchronously,
      // so this call returns immediately.
      PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, inputStream, new ObjectMetadata());
      // set the listener
      request.setGeneralProgressListener(new ProgressListener() {
        Long totalSize = 0L;

        @Override
        public void progressChanged(ProgressEvent progressEvent) {
          Long transferred = progressEvent.getBytesTransferred();
          totalSize = totalSize + transferred;
          System.out.println("total transferred bytes: " + totalSize);
        }
      });
      Upload upload = tm.upload(request);
      System.out.println("Object upload started");
      // Optionally, wait for the upload to finish before continuing.
      upload.waitForCompletion();
      System.out.println("Object upload complete");
    } catch (InterruptedException | SdkClientException e) {
      // The call was transmitted successfully, but Amazon S3 couldn't process
      // it, so it returned an error response.
      e.printStackTrace();
    }
  }

  @Override
  public UploadResult partUpload(String bucketName, String objectKey, File file) {
    ExecutorService pool = initExcutor();

    TransferManager tm = this.initTransferManager(pool);
    // TransferManager 采用异步方式进行处理，因此该调用会立即返回
    PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, file);

    // set the listener
    request.setGeneralProgressListener(new ProgressListener() {
      Long totalSize = 0L;

      @Override
      public void progressChanged(ProgressEvent progressEvent) {
        Long transferred = progressEvent.getBytesTransferred();
        totalSize += transferred;
        System.out.println("total transferred bytes: " + totalSize);
      }
    });

    Upload upload = tm.upload(request);
    UploadResult result;
    try {
      // 等待上传全部完成
      result = upload.waitForUploadResult();
      System.out.println("Upload complete.");

    } catch (AmazonClientException | InterruptedException e) {

      e.printStackTrace();
      throw new CephException(e.getMessage());
    } finally {
      this.shutdown(tm, pool);
    }
    return result;
  }

  @Override
  public UploadResult partUpload(String bucketName, String objectKey, InputStream input) {
    ExecutorService pool = initExcutor();
    // 初始化transfermanager
    TransferManager tm = this.initTransferManager(pool);

    //上传文件流
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(TransferManagerConf.MINIMUM_UPLOAD_PART_SIZE);
    // TransferManager 采用异步方式进行处理，因此该调用会立即返回
    PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, input, objectMetadata);

    // set the listener
    request.setGeneralProgressListener(new ProgressListener() {
      Long totalSize = 0L;

      @Override
      public void progressChanged(ProgressEvent progressEvent) {
        Long transferred = progressEvent.getBytesTransferred();
        totalSize = totalSize + transferred;
        System.out.println("total transferred bytes: " + totalSize);
      }
    });

    Upload upload = tm.upload(request);
    UploadResult result;
    try {
      // 等待上传全部完成
      result = upload.waitForUploadResult();
      System.out.println("Upload complete.");

    } catch (AmazonClientException | InterruptedException e) {

      e.printStackTrace();
      throw new CephException(e.getMessage());
    } finally {
      this.shutdown(tm, pool);
    }
    return result;
  }

  @Override
  public void lowLevelPartUpload(String bucketName, String objectKey, File file) {
    long contentLength = file.length();
    // Set part size to 5 MB
    long partSize = 5 * 1024 * 1024;
    // Create a list of ETag objects. You retrieve ETags for each object part uploaded,
    // then, after each individual part has been uploaded, pass the list of ETags to
    // the request to complete the upload.
    List<PartETag> partETags = new ArrayList<>();

    // Initiate the multipart upload.
    try {
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectKey);
      InitiateMultipartUploadResult initResponse = amazonS3.initiateMultipartUpload(initRequest);

      // Upload the file parts.
      long filePosition = 0;
      for (int i = 1; filePosition < contentLength; i++) {
        // Because the last part could be less than 5 MB, adjust the part size as needed.
        partSize = Math.min(partSize, (contentLength - filePosition));

        // Create the request to upload a part.
        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(objectKey)
                .withUploadId(initResponse.getUploadId())
                .withPartNumber(i)
                .withFileOffset(filePosition)
                .withFile(file)
                .withPartSize(partSize);

        // Upload the part and add the response's ETag to our list.
        UploadPartResult uploadResult = amazonS3.uploadPart(uploadRequest);
        partETags.add(uploadResult.getPartETag());
        filePosition += partSize;
      }
      CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName, objectKey, initResponse.getUploadId(), partETags);
      amazonS3.completeMultipartUpload(completeMultipartUploadRequest);
    } catch (SdkClientException e) {
      e.printStackTrace();
    }
  }

  private ExecutorService initExcutor() {
    // 创建一个线程池，大小为5
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("s3-part-upload-thread-%d").build();
    int size = TransferManagerConf.FIXED_THREAD_POOL;
    ExecutorService pool = new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), namedThreadFactory);
    return pool;
  }

  private TransferManager initTransferManager(ExecutorService pool) {
    /**
     * Constructs a new <code>TransferManager</code>,
     * specifying the client to use when making
     * requests to Amazon S3.
     * <p>
     * <code>TransferManager</code> and client objects
     * may pool connections and threads.
     * Reuse <code>TransferManager</code> and client objects
     * and share them throughout applications.
     * <p>
     * TransferManager and all AWS client objects are thread safe.
     * </p>
     */
    TransferManager transferManager = new TransferManager(amazonS3, pool);
    // 分块上传配置
    TransferManagerConfiguration configuration = new TransferManagerConfiguration();
    // 设置最小分片大小，默认是5MB。如果设置过小，会导致切片过多，影响上传速度。
    configuration.setMinimumUploadPartSize(TransferManagerConf.MINIMUM_UPLOAD_PART_SIZE);
    // 设置采用分片上传的阀值，只有当文件大于该值时，才会采用分片上传，否则采用普通上传。默认值是16MB
    configuration.setMultipartUploadThreshold(TransferManagerConf.MULTIPART_UPLOAD_THRESHOLD);
    transferManager.setConfiguration(configuration);
    return transferManager;
  }


  private void shutdown(TransferManager tm, ExecutorService pool) {
    tm.shutdownNow();
    pool.shutdown();
  }

}
