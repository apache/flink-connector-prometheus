/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink.aws;

import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.util.Preconditions;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.util.BinaryUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/** Sing a Remote-Write request to Amazon Managed Service for Prometheus (AMP). */
public class AmazonManagedPrometheusWriteRequestSigner implements PrometheusRequestSigner {

    private final URL remoteWriteUrl;
    private final String awsRegion;

    /**
     * Constructor.
     *
     * @param remoteWriteUrl URL of the remote-write endpoint
     * @param awsRegion Region of the AMP workspace
     */
    public AmazonManagedPrometheusWriteRequestSigner(String remoteWriteUrl, String awsRegion) {
        Preconditions.checkArgument(StringUtils.isNotBlank(awsRegion));
        Preconditions.checkNotNull(remoteWriteUrl);
        this.awsRegion = awsRegion;
        try {
            this.remoteWriteUrl = new URL(remoteWriteUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid Remote-Write URL: " + remoteWriteUrl, e);
        }
    }

    /**
     * Add the additional Http request headers required by Amazon Managed Prometheus:
     * 'x-amz-content-sha256', 'Host', 'X-Amz-Date', 'x-amz-security-token' and 'Authorization`.
     *
     * @param requestHeaders original Http request headers. It must be mutable. For efficiency, any
     *     new header is added to the map, instead of making a copy.
     * @param requestBody request body, already compressed
     */
    @Override
    public void addSignatureHeaders(Map<String, String> requestHeaders, byte[] requestBody) {
        byte[] contentHash = AWS4SignerBase.hash(requestBody);
        String contentHashString = BinaryUtils.toHex(contentHash);
        requestHeaders.put(
                "x-amz-content-sha256",
                contentHashString); // this header must be included before generating the
        // Authorization header

        DefaultAWSCredentialsProviderChain credsChain = new DefaultAWSCredentialsProviderChain();
        AWSCredentials awsCreds = credsChain.getCredentials();
        String sessionToken =
                (awsCreds instanceof AWSSessionCredentials)
                        ? ((AWSSessionCredentials) awsCreds).getSessionToken()
                        : null;

        AWS4SignerForAuthorizationHeader signer =
                new AWS4SignerForAuthorizationHeader(remoteWriteUrl, "POST", "aps", awsRegion);
        // computeSignature also adds 'Host', 'X-Amz-Date' and 'x-amz-security-token' to the
        // requestHeaders Map
        String authorization =
                signer.computeSignature(
                        requestHeaders,
                        null, // no query parameters
                        contentHashString,
                        awsCreds.getAWSAccessKeyId(),
                        awsCreds.getAWSSecretKey(),
                        sessionToken);
        requestHeaders.put("Authorization", authorization);
    }
}
