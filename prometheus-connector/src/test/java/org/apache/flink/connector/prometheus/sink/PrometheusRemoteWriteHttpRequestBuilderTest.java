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
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PrometheusRemoteWriteHttpRequestBuilderTest {

    private static final String ENDPOINT = "/anything";
    private static final byte[] REQUEST_BODY = {(byte) 0x01};

    @Test
    void shouldAddContentEncodingHeader() {
        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, null);
        var request = sut.buildHttpRequest(REQUEST_BODY);
        assertEquals("snappy", request.getHeaders(HttpHeaders.CONTENT_ENCODING)[0].getValue());
    }

    @Test
    void shouldAddPrometheusRemoteWriteVersionHeader() {
        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, null);
        var request = sut.buildHttpRequest(REQUEST_BODY);
        assertEquals(
                "0.1.0", request.getHeaders("X-Prometheus-Remote-Write-Version")[0].getValue());
    }

    @Test
    void shouldAddUserAgent() {
        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, null);
        var request = sut.buildHttpRequest(REQUEST_BODY);
        assertEquals(1, request.getHeaders(HttpHeaders.USER_AGENT).length);
    }

    @Test
    void shouldInvokeRequestSignerPassingAMutableMap() {
        PrometheusRequestSigner mockSigner = mock(PrometheusRequestSigner.class);
        PrometheusRemoteWriteHttpRequestBuilder sut =
                new PrometheusRemoteWriteHttpRequestBuilder(ENDPOINT, mockSigner);

        sut.buildHttpRequest(REQUEST_BODY);

        ArgumentCaptor<Map<String, String>> headerMapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockSigner).addSignatureHeaders(headerMapCaptor.capture(), eq(REQUEST_BODY));

        headerMapCaptor
                .getValue()
                .put("foo", "bar"); // Check if the map passed to the signer is mutable
    }
}
