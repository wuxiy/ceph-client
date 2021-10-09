/*********************************************************************************
 * Copyright (c)2020 CEC Health
 * FILE: CS3Builder
 * 版本      DATE             BY               REMARKS
 * ----  -----------  ---------------  ------------------------------------------
 * 1.0   2020-06-08        xiwu
 ********************************************************************************/
package org.aaa.ceph.service;

import com.google.common.base.Strings;

import java.util.Arrays;

/**
 * common simple storage service builder
 * Fluent builder for {@link CS3Builder}. Use of the builder is preferred over using constructors of
 * the client class.
 * @Author: wuxi
 * @Date: 2019/3/13
 */
public class CS3Builder {
    /**
     * access_key
     */
    private String accessKey;
    /**
     * secret_key
     */
    private String secretKey;
    /**
     * the endpoint to be used for requests.
     * <p>For example: http://127.0.0.1:80
     */
    private String endpoint;

    /**
     * Sets the access key to be used by the client.
     *
     * @param accessKey Access key to use.
     * @return This object for method chaining.
     */
    public CS3Builder accessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    /**
     * Sets the secret key to be used by the client.
     *
     * @param secretKey Secret key to use.
     * @return This object for method chaining.
     */
    public CS3Builder secretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    /**
     * Sets the endpoint to be used for requests.
     *
     * <p>For example: http://127.0.0.1:80
     *
     * @param endpoint Endpoint to use.
     * @return This object for method chaining.
     */
    public  CS3Builder endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    /**
     * Builds a client with the configure properties.
     *
     * @return Client instance to make API calls with.
     */
    public CS3Service build() {
        if (Arrays.asList(accessKey, secretKey, endpoint).stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new IllegalArgumentException("Missing required parameter to build the instance.");
        }
        return new CS3ServiceImpl(accessKey, secretKey, endpoint);
    }


}
