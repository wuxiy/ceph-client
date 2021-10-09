package org.aaa.ceph.client;

import cn.hutool.core.codec.Base64;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Strings;
import org.aaa.ceph.constant.TransferManagerConf;
import org.aaa.ceph.exception.CephException;
import org.aaa.ceph.service.CS3Builder;
import org.aaa.ceph.service.CS3Service;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * effective java recommended singleton
 * Best to instantiate once
 * @Author: wuxi
 * @Date: 2019/3/14
 */
public enum CS3ClientAdmin implements CS3ClientInterface {
    INSTANCE {
        /**
         * user's access_key
         */
        private String accessKey = "VCQY7SF1I6IICFYARYUT";
        /**
         * user's secret_key
         */
        private String secretKey = "Y9KWiLCP8RtwvmMsnhXUxtL13pXPw61WPjiFXC74";
        /**
         * the endpoint to be used for requests.
         * <p>For example: ceph rgw's ip
         */
        private String endpoint = "http://172.16.57.50:8080";

        /**
         * user's default bucket name
         */
        private static final String DEFAULT_BUCKET = "default";

        private static final String DEFAULT_SEPARATOR = ",";


        /**
         * default user id
         */
        private static final String DEFAULT_USER_ID = "test";

        /**
         * user namespace
         */
        private String userId;

        /**
         * bucket
         */
        private String bucket;


        private org.aaa.ceph.service.CS3Service CS3Service;


        /**
         * bucketName
         * @param bucketName
         * @return bucket userId + "." + bucketName
         */
        public String getBucketName(String bucketName) {
            return this.userId + "." + bucketName;
        }

        /**
         * objectKey
         * @param bucketName
         * @param objectKey
         * @return
         */
        public String getObjectKey(String bucketName, String objectKey) {
            return Base64.encode(bucketName) + DEFAULT_SEPARATOR + objectKey;
        }

        /**
         * decoding of object identifier
         * @param objectKey
         * @return list of bucketKey and objectKey
         */
        public List<String> decodeObjectKey(String objectKey) {
            int index = objectKey.indexOf(DEFAULT_SEPARATOR);
            List<String> strs = new ArrayList<>();
            strs.add(Base64.decodeStr(objectKey.substring(0,index)));
            strs.add(objectKey.substring(index+1));
            return strs;
        }

        /**
         * Builds a client with the configure properties.
         *
         * @return Client instance to make API calls with.
         */
        public void build() {
            if (Arrays.asList(accessKey, secretKey, endpoint).stream().anyMatch(Strings::isNullOrEmpty)) {
                throw new IllegalArgumentException("Missing required parameter to build the instance.");
            }
            this.CS3Service =  new CS3Builder()
                    .accessKey(this.accessKey)
                    .secretKey(this.secretKey)
                    .endpoint(this.endpoint)
                    .build();
        }

        /**
         *
         * @param userId
         */
        public void handler(String userId, String bucket) {
            this.userId = userId;
            // init s3 service
            this.build();
            this.bucket = bucket;
            //  If it is the first time you create a user，need to create a default bucket
            CS3Service.createBucket(getBucketName(bucket));

        }

        /**
         * init client with userId
         * @param userId Need to bring uid for each request
         * @return this class
         */
        @Override
        public CS3ClientInterface init(String userId) {
            handler(userId, DEFAULT_BUCKET);
            return this;
        }
        /**
         * init client without userId
         *
         * @return this class
         */
        @Override
        public CS3ClientInterface init() {
            handler(DEFAULT_USER_ID, DEFAULT_BUCKET);
            return this;
        }

        @Override
        public CS3ClientInterface build(String accessKey, String secretKey, String endpoint) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.endpoint = endpoint;
            handler(DEFAULT_USER_ID, DEFAULT_BUCKET);
            return this;
        }

        @Override
        public CS3ClientInterface build(String accessKey, String secretKey, String endpoint, String bucket) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.endpoint = endpoint;
            handler(DEFAULT_USER_ID, bucket);
            return this;
        }

        @Override
        public CS3ClientInterface build(String accessKey, String secretKey, String endpoint, String bucket, String userId) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.endpoint = endpoint;
            handler(userId, bucket);
            return this;
        }

        @Override
        public CS3ClientInterface initWithBucket(String bucket) {
            handler(DEFAULT_USER_ID, bucket);
            return this;
        }

        @Override
        public String createObject(File file) {
            String bucketKey = getBucketName(bucket);
            String objectKey = UUID.randomUUID().toString();
            CS3Service.createObject(bucketKey, objectKey ,file);
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public String createObject(String bucketName,File file) {
            String bucketKey = getBucketName(bucketName);
            CS3Service.createBucket(bucketKey);
            String objectKey = UUID.randomUUID().toString();
            CS3Service.createObject(bucketKey, objectKey, file);
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public String createObject(InputStream input) {
            String bucketKey = getBucketName(bucket);
            String objectKey = UUID.randomUUID().toString();
            CS3Service.createObject(bucketKey, objectKey, input);
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public String createObject(String bucketName,InputStream input) {
            String bucketKey = getBucketName(bucketName);
            CS3Service.createBucket(bucketKey);
            String objectKey = UUID.randomUUID().toString();
            CS3Service.createObject(bucketKey, objectKey,input);
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public String upload(InputStream input, Long size) {
            String bucketKey = getBucketName(bucket);
            String objectKey = UUID.randomUUID().toString();
            if (size > TransferManagerConf.MULTIPART_UPLOAD_THRESHOLD) {
                CS3Service.highLevelPartUpload(bucketKey, objectKey, input);
            } else {
                CS3Service.createObject(bucketKey, objectKey, input);
            }
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public String upload(File file, Long size) {
            String bucketKey = getBucketName(bucket);
            String objectKey = UUID.randomUUID().toString();
            if (size > TransferManagerConf.MULTIPART_UPLOAD_THRESHOLD) {
                CS3Service.highLevelPartUpload(bucketKey, objectKey, file);
            } else {
                CS3Service.createObject(bucketKey, objectKey, file);
            }
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public String partUpload(String bucketName, InputStream input) {
            String bucketKey = getBucketName(bucketName);
            CS3Service.createBucket(bucketKey);
            String objectKey = UUID.randomUUID().toString();
            CS3Service.partUpload(bucketKey, objectKey,input);
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public String partUpload(String bucketName, File file) {
            String bucketKey = getBucketName(bucketName);
            CS3Service.createBucket(bucketKey);
            String objectKey = UUID.randomUUID().toString();
            CS3Service.lowLevelPartUpload(bucketKey, objectKey, file);
            return getObjectKey(bucketKey, objectKey);
        }

        @Override
        public void getObject(String objectKey,File file) {
            List<String> strs = decodeObjectKey(objectKey);
            if(strs.isEmpty()) {
                throw new CephException("objectKey has error!");
            }
            CS3Service.getObject(strs.get(0), strs.get(1), file);
        }

        @Override
        public void getObject(String bucketName,String objectKey,File file) {
            CS3Service.getObject(getBucketName(bucketName), objectKey,file);
        }

        @Override
        public InputStream getObject(String objectKey) {
            List<String> strs = decodeObjectKey(objectKey);
            if(strs.isEmpty()) {
                throw new CephException("obejctKey has error!");
            }

            S3Object object = CS3Service.getObject(strs.get(0), strs.get(1));
            return object.getObjectContent();
        }

        @Override
        public InputStream getObject(String bucketName,String objectKey) {
            return CS3Service.getObject(getBucketName(bucketName), objectKey).getObjectContent();
        }

        @Override
        public void removeObject(String objectKey) {
            List<String> strs = decodeObjectKey(objectKey);
            if(strs.isEmpty()) {
                throw new CephException("obejctKey has error!");
            }
            CS3Service.removeObject(strs.get(0), strs.get(1));
        }

        @Override
        public List<S3ObjectSummary> listObject(String bucektName) {
            return CS3Service.listObjects(bucektName).getObjectSummaries();
        }

        @Override
        public List<S3ObjectSummary> listObject() {
            return CS3Service.listObjects(getBucketName(DEFAULT_BUCKET)).getObjectSummaries();
        }


    };

    public static CS3ClientInterface getInstance() {

        return CS3ClientAdmin.INSTANCE.init();
    }

    public static CS3ClientInterface getInstance(String accessKey, String secretKey, String endpoint) {

        return CS3ClientAdmin.INSTANCE.build(accessKey, secretKey, endpoint);
    }

    public static CS3ClientInterface getInstance(String accessKey, String secretKey, String endpoint, String bucket) {

        return CS3ClientAdmin.INSTANCE.build(accessKey, secretKey, endpoint, bucket);
    }

    public static CS3ClientInterface getInstance(String accessKey, String secretKey, String endpoint, String bucket, String userId) {

        return CS3ClientAdmin.INSTANCE.build(accessKey, secretKey, endpoint, bucket, userId);
    }

    public CS3ClientInterface getInstanceWithBucket(String bucket) {
        return CS3ClientAdmin.INSTANCE.initWithBucket(bucket);
    }

    public static CS3ClientInterface getInstance(String userId) {
        return CS3ClientAdmin.INSTANCE.init(userId);
    }
}
