package org.aaa.ceph.exception;


/**
 * @Author: wuxi
 * @Date: 2019/3/14
 */
public class CephException extends RuntimeException {
    public CephException() {
    }

    public CephException(String message) {
        super(message);
    }

    public CephException(String message, Throwable cause) {
        super(message, cause);
    }

}
