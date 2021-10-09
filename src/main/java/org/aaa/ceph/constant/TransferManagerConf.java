package org.aaa.ceph.constant;

/**
 * @Author: wuxi
 * @Date: 2019/3/19
 */
public interface TransferManagerConf {

    /**
     * 最小分片大小
     */
    Long MINIMUM_UPLOAD_PART_SIZE = 10 * 1024 * 1024L;

    /**
     * 分片大小上传的阀值
     */
    Long MULTIPART_UPLOAD_THRESHOLD = 20 * 1024 * 1024L;

    /**
     * 线程池大小数量
     */
    int FIXED_THREAD_POOL = 5;


}
