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

import org.apache.flink.util.Preconditions;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;

import java.util.HashMap;
import java.util.Map;

/** Builds the POST request to the Remote-Write endpoint for a given binary payload. */
public class PrometheusRemoteWriteHttpRequestBuilder {

    private static final ContentType CONTENT_TYPE = ContentType.create("application/x-protobuf");

    private static final String CONTENT_ENCODING = "snappy";
    private static final String REMOTE_WRITE_VERSION_HEADER = "X-Prometheus-Remote-Write-Version";
    private static final String REMOTE_WRITE_VERSION = "0.1.0";

    public static final String DEFAULT_USER_AGENT = "Flink-Prometheus";

    private final String prometheusRemoteWriteUrl;
    private final PrometheusRequestSigner requestSigner;

    private final Map<String, String> fixedHeaders;

    public PrometheusRemoteWriteHttpRequestBuilder(
            String prometheusRemoteWriteUrl,
            PrometheusRequestSigner requestSigner,
            String httpUserAgent) {
        Preconditions.checkNotNull(httpUserAgent, "User-Agent not specified");

        this.prometheusRemoteWriteUrl = prometheusRemoteWriteUrl;
        this.requestSigner = requestSigner;
        this.fixedHeaders = new HashMap<>();
        fixedHeaders.put(HttpHeaders.CONTENT_ENCODING, CONTENT_ENCODING);
        fixedHeaders.put(REMOTE_WRITE_VERSION_HEADER, REMOTE_WRITE_VERSION);
        fixedHeaders.put(HttpHeaders.USER_AGENT, httpUserAgent);
    }

    public SimpleHttpRequest buildHttpRequest(byte[] httpRequestBody) {
        Map<String, String> headers = new HashMap<>(fixedHeaders);
        if (requestSigner != null) {
            requestSigner.addSignatureHeaders(headers, httpRequestBody);
        }

        SimpleRequestBuilder builder =
                SimpleRequestBuilder.post()
                        .setUri(prometheusRemoteWriteUrl)
                        .setBody(httpRequestBody, CONTENT_TYPE);

        for (Map.Entry<String, String> header : headers.entrySet()) {
            builder.addHeader(header.getKey(), header.getValue());
        }

        return builder.build();
    }
}
