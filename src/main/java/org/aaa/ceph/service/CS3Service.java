/*********************************************************************************
 * Copyright (c)2020 CEC Health
 * FILE: CS3Service
 * 版本      DATE             BY               REMARKS
 * ----  -----------  ---------------  ------------------------------------------
 * 1.0   2020-06-08        xiwu
 ********************************************************************************/
package org.aaa.ceph.service;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.model.UploadResult;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * common simple storage service Service
 * <p>Administer the Ceph Object Storage (a.k.a. Radosgw) service with user management, access
 * controls, quotas and usage tracking among other features.
 *
 * @Author: wuxi
 * @Date: 2019/3/13
 */
public interface CS3Service {

    /**
     * Get information about buckets under a given user.
     *
     * @return The desired bucket information.
     */
    List<Bucket> listBuckets();

    /**
     * create a new bucket under a given user
     * <p>
     *    Bucket is a container for storing Objects, and each Object must be stored in a specific bucket.
     *    In the Ceph RGW, each user can create up to 1000 buckets, and each bucket can store an unlimited number of objects.
     * </p>
     * @param bucketName
     * @return the bucket information.
     */
    Bucket createBucket(String bucketName);

    /**
     * remove a bucket under a given user
     *
     * @param bucketName
     * @return the removed bucket information.
     */
    void removeBucket(String bucketName);

    /**
     * list objects under a given bucket
     * @param bucketName
     * @return the bucket list information.
     */
    ObjectListing listObjects(String bucketName);

    /**
     * create a new object under a given bucket
     * by local file e.g., new File("localFile")
     * @param bucketName The specified bucket.
     * @param objectKey Unique identifier e.g., UUID
     * @param file local file.
     * @return S3 Object.
     */
    S3ObjectSummary createObject(String bucketName, String objectKey, File file);


    /**
     * create a new object under a given bucket
     * by file stream e.g., new FileInputStream("localFile").
     *
     * @param bucketName The specified bucket.
     * @param objectKey Unique identifier e.g., UUID.
     * @param input Input file stream.
     * @return
     */
    S3ObjectSummary createObject(String bucketName, String objectKey, InputStream input);


    /**
     * Get the specified object.
     *
     * @param bucketName The specified bucket.
     * @param objectKey  The specified unique identifier.
     * @return S3 Object
     */
    S3Object getObject(String bucketName, String objectKey);

    /**
     * Get the specified object to local file
     *
     * @param bucketName The specified bucket.
     * @param objectKey The specified unique identifier.
     * @param file local file
     */
    void getObject(String bucketName, String objectKey, File file);


    /**
     * Remove an existing object.
     *
     * @param bucketName The bucket containing the object to be removed.
     * @param objectKey The object to remove.
     * @return S3 Object
     */
    void removeObject(String bucketName, String objectKey);


    /**
     * part upload
     * <p>The simple upload method can only upload objects smaller than 5GB. If you need to upload objects larger than 5GB,
     * you must use the slice upload mode. In addition,you can also use the slice upload mode in the following application scenarios (but not limited to this):
     * <li> Need to support breakpoint resume.
     * <li> Upload files over 100MB in size.
     * <li> The size of the uploaded file cannot be determined until the file is uploaded.
     * <li> Higher throughput is required (using concurrent upload shards).
     * <li> The network conditions are poor and the links between the servers are often broken.
     *
     * <p> low level
     *  1. initiateMultipartUpload
     *  2. UploadPart
     *  3. CompleteMultipartUpload
     *  <p> high level
     *  1. create TransferManager
     *  2. TransferManager.upload
     *
     * @param bucketName The specified bucket.
     * @param objectKey The specified unique identifier.
     * @param file local file
     * @return
     */
    UploadResult partUpload(String bucketName, String objectKey, File file);

    /**
     * 分片上传文件流
     * @param bucketName
     * @param objectKey
     * @param input
     * @return
     */
    UploadResult partUpload(String bucketName, String objectKey, InputStream input);


    /**
     * low level part upload
     * @param bucketName
     * @param objectKey
     * @param file
     */
    void lowLevelPartUpload(String bucketName, String objectKey, File file);

    /**
     * 分片上传文件流
     * @param bucketName
     * @param objectKey
     * @param input
     * @return
     */
    void highLevelPartUpload(String bucketName, String objectKey, InputStream input);

    /**
     * 分片上传文件流
     * @param bucketName
     * @param objectKey
     * @param file
     * @return
     */
    void highLevelPartUpload(String bucketName, String objectKey, File file);


}
