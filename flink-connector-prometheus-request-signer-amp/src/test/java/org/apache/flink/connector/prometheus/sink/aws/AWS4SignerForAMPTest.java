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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.prometheus.sink.aws.RequestHeaderTestUtil.assertContainsHeader;
import static org.apache.flink.connector.prometheus.sink.aws.RequestHeaderTestUtil.assertDoesNotContainHeader;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AWS4SignerForAMPTest {

    private final AWS4SignerForAMP signer =
            new AWS4SignerForAMP(new URL("http://example.com/endpoint"), "us-east-1");

    public AWS4SignerForAMPTest() throws MalformedURLException {}

    @Test
    public void shouldAddExpectedHeaders_noSessionToken() throws Exception {

        Map<String, String> headers = new HashMap<>();

        signer.computeSignature(headers, null, "body-hash", "access-key", "secret-key", null);

        assertContainsHeader("x-amz-date", headers);
        assertContainsHeader("Host", headers);
        assertDoesNotContainHeader("x-amz-security-token", headers);
    }

    @Test
    public void shouldAddExpectedHeaders_sessionToken() throws Exception {
        Map<String, String> headers = new HashMap<>();

        signer.computeSignature(
                headers, null, "body-hash", "access-key", "secret-key", "session-token");

        assertContainsHeader("x-amz-date", headers);
        assertContainsHeader("Host", headers);
        assertContainsHeader("x-amz-security-token", headers);
    }

    @Test
    public void signatureShouldMatchPattern_SessionToken() throws Exception {
        String signature =
                signer.computeSignature(
                        new HashMap<>(),
                        null,
                        "body-hash",
                        "ACC355K3Y",
                        "secret-key",
                        "session-token");

        String expectedSignaturePatternWirthSecurityToken =
                "AWS4-HMAC-SHA256 Credential=([A-Za-z0-9]+)/([0-9]{8})/([a-z0-9-]+)/aps/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token, Signature=([a-f0-9]{64})";

        assertTrue(signature.matches(expectedSignaturePatternWirthSecurityToken));
    }

    @Test
    public void signatureShouldMatchPattern_noSessionToken() {
        String signature =
                signer.computeSignature(
                        new HashMap<>(), null, "body-hash", "ACC355K3Y", "secret-key", null);

        String expectedSignaturePatternWithoutSecurityToken =
                "AWS4-HMAC-SHA256 Credential=([A-Za-z0-9]+)/([0-9]{8})/([a-z0-9-]+)/aps/aws4_request, SignedHeaders=host;x-amz-date, Signature=([a-f0-9]{64})";

        assertTrue(signature.matches(expectedSignaturePatternWithoutSecurityToken));
    }
}
