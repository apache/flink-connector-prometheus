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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.prometheus.sink.aws.RequestHeaderTestUtil.assertContainsHeader;
import static org.apache.flink.connector.prometheus.sink.aws.RequestHeaderTestUtil.assertDoesNotContainHeader;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AmazonManagedPrometheusWriteRequestSignerTest {

    @Test
    public void constructorShouldFailIfRemoteWriteUrlIsBlankString() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    new AmazonManagedPrometheusWriteRequestSigner(" ", "us-east-1");
                });
    }

    @Test
    public void constructorShouldFailIfRemoteWriteUrlIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    new AmazonManagedPrometheusWriteRequestSigner(null, "us-east-1");
                });
    }

    @Test
    public void constructorShouldSucceedIfRemoteWriteUrlIsValidURL() {
        assertDoesNotThrow(
                () -> {
                    new AmazonManagedPrometheusWriteRequestSigner(
                            "https://example.com", "us-east-1");
                });
    }

    @Test
    public void constructorShouldFaiIfRemoteWriteUrlIsInvalidURL() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    new AmazonManagedPrometheusWriteRequestSigner("invalid-url", "us-east-1");
                });
    }

    @Test
    public void constructorShouldFaiIfRegionIsBlankString() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    new AmazonManagedPrometheusWriteRequestSigner("https://example.com", " ");
                });
    }

    @Test
    public void constructorShouldFaiIfRegionIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    new AmazonManagedPrometheusWriteRequestSigner("https://example.com", null);
                });
    }

    @Test
    public void shouldAddExpectedHeaders_BasicCredentials() {
        AmazonManagedPrometheusWriteRequestSigner signer =
                new AmazonManagedPrometheusWriteRequestSigner(
                        "https://example.com/endpoint",
                        "us-east-1",
                        new DummyAwsBasicCredentialsProvider("access-key-id", "secret-access-key"));

        Map<String, String> requestHeaders = new HashMap<>();
        byte[] requestBody = "request-payload".getBytes(StandardCharsets.UTF_8);

        signer.addSignatureHeaders(requestHeaders, requestBody);

        assertContainsHeader("Authorization", requestHeaders);
        assertContainsHeader("x-amz-content-sha256", requestHeaders);
        assertContainsHeader("x-amz-date", requestHeaders);
        assertContainsHeader("Host", requestHeaders);
        // The security token is only expected with session credentials
        assertDoesNotContainHeader("x-amz-security-token", requestHeaders);
    }

    @Test
    public void shouldAddExpectedHeaders_SessionCredentials() {
        AmazonManagedPrometheusWriteRequestSigner signer =
                new AmazonManagedPrometheusWriteRequestSigner(
                        "https://example.com/endpoint",
                        "us-east-1",
                        new DummAwsSessionCredentialProvider(
                                "access-key-id", "secret-access-key", "session-key"));

        Map<String, String> requestHeaders = new HashMap<>();
        byte[] requestBody = "request-payload".getBytes(StandardCharsets.UTF_8);

        signer.addSignatureHeaders(requestHeaders, requestBody);

        assertContainsHeader("Authorization", requestHeaders);
        assertContainsHeader("x-amz-content-sha256", requestHeaders);
        assertContainsHeader("x-amz-date", requestHeaders);
        assertContainsHeader("Host", requestHeaders);
        // With Session credentials should have the securitu token
        assertContainsHeader("x-amz-security-token", requestHeaders);
    }
}
