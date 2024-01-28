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

package org.apache.flink.connector.prometheus.sink;

import org.apache.hc.core5.http.HttpHeaders;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrometheusRemoteWriteHttpRequestBuilderTest {

    private static final String ENDPOINT = "/anything";
    private static final byte[] REQUEST_BODY = {(byte) 0x01};

    private static final String USER_AGENT = "MY-USER-AGENT";

    @Test
    void shouldAddContentEncodingHeader() {
        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, null, USER_AGENT);
        var request = sut.buildHttpRequest(REQUEST_BODY);
        assertEquals("snappy", request.getHeaders(HttpHeaders.CONTENT_ENCODING)[0].getValue());
    }

    @Test
    void shouldAddPrometheusRemoteWriteVersionHeader() {
        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, null, USER_AGENT);
        var request = sut.buildHttpRequest(REQUEST_BODY);
        assertEquals(
                "0.1.0", request.getHeaders("X-Prometheus-Remote-Write-Version")[0].getValue());
    }

    @Test
    void shouldAddUserAgent() {
        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, null, USER_AGENT);
        var request = sut.buildHttpRequest(REQUEST_BODY);
        assertEquals(1, request.getHeaders(HttpHeaders.USER_AGENT).length);
        assertEquals(USER_AGENT, request.getHeaders(HttpHeaders.USER_AGENT)[0].getValue());
    }

    @Test
    void shouldInvokeRequestSignerPassingAMutableMap() {
        CapturingPrometheusRequestSigner signer = new CapturingPrometheusRequestSigner();

        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, signer, USER_AGENT);

        sut.buildHttpRequest(REQUEST_BODY);

        // Verify the signer was invoked once
        assertEquals(1, signer.getInvocationCount());

        // Verify the header Map of headers passed to the signer was actually mutable
        Map<String, String> capturedRequestHeaders = signer.getRequestHeadersAtInvocationCount(1);
        capturedRequestHeaders.put("foo", "bar");
        assertEquals("bar", capturedRequestHeaders.get("foo"));
    }
}
