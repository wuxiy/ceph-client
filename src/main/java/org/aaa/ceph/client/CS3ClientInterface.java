package org.aaa.ceph.client;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * non-singleton
 * @Author: wuxi
 * @Date: 2019/3/14
 */
public interface CS3ClientInterface {

    /**
     *  upload object in the default bucket
     *  the default bucket is "default".
     * @param file local file e.g., new File("filepath")
     * @return objectKey e.g., UUID
     */
    String createObject(File file);

    /**
     * init client without userId
     *
     * @return this class
     */
    CS3ClientInterface init();


    /**
     * build client
     * @param accessKey
     * @param secretKey
     * @param endpoint ceph rgw's ip
     * @return
     */
    CS3ClientInterface build(String accessKey, String secretKey, String endpoint);

    /**
     * build client
     * @param accessKey
     * @param secretKey
     * @param endpoint ceph rgw's ip
     * @param bucket 指定bucket
     * @return
     */
    CS3ClientInterface build(String accessKey, String secretKey, String endpoint, String bucket);


    /**
     * build client
     * @param accessKey
     * @param secretKey
     * @param endpoint ceph rgw's ip
     * @param bucket 指定bucket，装载对象
     * @param userId 指定用户
     * @return
     */
    CS3ClientInterface build(String accessKey, String secretKey, String endpoint, String bucket, String userId);
    /**
     * init client with userId
     * @param userId Need to bring uid for each request
     * @return this class
     */
    CS3ClientInterface init(String userId);

    /**
     * init client with bucket
     * @param bucket
     * @return
     */
    CS3ClientInterface initWithBucket(String bucket);

    /**
     *  part upload object in the specified bucket.
     *
     * @param bucketName The specified bucket.
     * @param file local file e.g., new File("filepath")
     * @return objectKey e.g., UUID
     */
    String createObject(String bucketName, File file);

    /**
     * upload object in the default bucket
     * the default bucket is "default",
     * @param input Input stream.
     * @return objectKey.
     */
    String createObject(InputStream input);

    /**
     * upload object in the specified bucket.
     *
     * @param bucketName The specified bucket.
     * @param input Input stream.
     * @return objectKey.
     */
    String createObject(String bucketName, InputStream input);

    /**
     * upload object in the specified bucket.
     *
     * @param input Input stream.
     * @param size file size
     * @return objectKey.
     */
    String upload(InputStream input, Long size);

    /**
     * upload object in the specified bucket.
     *
     * @param file file.
     * @param size file size
     * @return objectKey.
     */
    String upload(File file, Long size);


    /**
     * upload object in the specified bucket.
     *
     * @param bucketName The specified bucket.
     * @param input Input stream.
     * @return objectKey.
     */
    String partUpload(String bucketName, InputStream input);


    /**
     *  part upload object in the specified bucket.
     *
     * @param bucketName The specified bucket.
     * @param file local file e.g., new File("filepath")
     * @return objectKey e.g., UUID
     */
    String partUpload(String bucketName, File file);

    /**
     *  get object to local file in the default bucket.
     *
     * @param file local file e.g., new File("filepath").
     */
    void getObject(String objectKey, File file);

    /**
     *  get object in the specified bucket.
     *
     * @param bucketName The specified bucket.
     * @param objectKey  Object unique identifier.
     * @param file local file e.g., new File("filepath").
     */
    void getObject(String bucketName, String objectKey, File file);

    /**
     * get object in the default bucket.
     *
     * the default bucket is "default",
     * @param objectKey Object unique identifier.
     * @return InputStream to read data.
     */
    InputStream getObject(String objectKey);

    /**
     * get object in the specified bucket.
     *
     * @param bucketName The specified bucket.
     * @param objectKey Input stream.
     * @return InputStream to read data.
     */
    InputStream getObject(String bucketName, String objectKey);

    /**
     * remove obejct
     * @param objectKey
     */
    void removeObject(String objectKey);

    /**
     * list object by The specified bucket
     * default bucketName is "cechealth.default"
     * @param bucektName
     * @return
     */
    List<S3ObjectSummary> listObject(String bucektName);

    /**
     * list object by The default bucket is "******.default"
     * @return
     */
    List<S3ObjectSummary> listObject();


}
