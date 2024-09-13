/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink.aws;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * Sign a Remote-Write request to Amazon Managed Service for Prometheus (AMP).
 *
 * <p>On every request, AWS credentials are retrieved using an AwsCredentialsProvider, and used to
 * sign the request, with AWS Signature Version 4a.
 */
@PublicEvolving
public class AmazonManagedPrometheusWriteRequestSigner implements PrometheusRequestSigner {
    // Header names
    private static final String X_AMZ_CONTENT_SHA_256 = "x-amz-content-sha256";
    private static final String AUTHORIZATION = "Authorization";

    private final URL remoteWriteUrl;
    private final String awsRegion;

    private final AwsCredentialsProvider awsCredProvider;

    /**
     * Creates a signer instance with a specified credential provider.
     *
     * @param remoteWriteUrl URL of the remote-write endpoint
     * @param awsRegion Region of the AMP workspace
     * @param awsCredProvider implementation of AwsCredentialsProvider to retrieve the credentials
     */
    public AmazonManagedPrometheusWriteRequestSigner(
            String remoteWriteUrl, String awsRegion, AwsCredentialsProvider awsCredProvider) {
        Preconditions.checkArgument(
                StringUtils.isNotBlank(awsRegion), "awsRegion cannot be null or empty");
        Preconditions.checkArgument(
                StringUtils.isNotBlank(remoteWriteUrl), "remoteWriteUrl cannot be null or empty");
        Preconditions.checkArgument(awsCredProvider != null, "credentialsProvider cannot be null");

        this.awsRegion = awsRegion;
        try {
            this.remoteWriteUrl = new URL(remoteWriteUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    "Invalid AMP remote-write URL: " + remoteWriteUrl, e);
        }
        this.awsCredProvider = awsCredProvider;
    }

    /**
     * Creates a signer instance using the default AWS credentials provider chain.
     *
     * @param remoteWriteUrl URL of the remote-write endpoint
     * @param awsRegion Region of the AMP workspace
     */
    public AmazonManagedPrometheusWriteRequestSigner(String remoteWriteUrl, String awsRegion) {
        this(remoteWriteUrl, awsRegion, DefaultCredentialsProvider.create());
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
        byte[] contentHash = AWS4SignerForAMP.hash(requestBody);
        String contentHashString = AWS4SignerForAMP.toHex(contentHash);

        // x-amz-content-sha256 must be included before generating the Authorization header
        requestHeaders.put(X_AMZ_CONTENT_SHA_256, contentHashString);

        // Get the credentials from the default credential provider chain
        AwsCredentials awsCreds = awsCredProvider.resolveCredentials();

        // If the credentials are from a session, also get the session token
        String sessionToken =
                (awsCreds instanceof AwsSessionCredentials)
                        ? ((AwsSessionCredentials) awsCreds).sessionToken()
                        : null;

        AWS4SignerForAMP signer = new AWS4SignerForAMP(remoteWriteUrl, awsRegion);

        // computeSignature also adds 'Host', 'X-Amz-Date' and 'x-amz-security-token' to the
        // requestHeaders Map
        String authorization =
                signer.computeSignature(
                        requestHeaders,
                        null, // no query parameters
                        contentHashString,
                        awsCreds.accessKeyId(),
                        awsCreds.secretAccessKey(),
                        sessionToken);
        requestHeaders.put(AUTHORIZATION, authorization);
    }
}
