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

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;

import java.util.HashMap;
import java.util.Map;

/** Builds the POST request to the Remote-Write endpoint for a given binary payload. */
public class PrometheusRemoteWriteHttpRequestBuilder {

    private static final ContentType CONTENT_TYPE = ContentType.create("application/x-protobuf");

    private static final Map<String, String> FIXED_HEADER =
            Map.of(
                    HttpHeaders.CONTENT_ENCODING,
                    "snappy",
                    "X-Prometheus-Remote-Write-Version",
                    "0.1.0",
                    HttpHeaders.USER_AGENT,
                    "Flink-Prometheus/0.1.0" // TODO Prometheus requires a user-agent header. What
                    // should we use?
                    );

    private final String prometheusRemoteWriteUrl;
    private final PrometheusRequestSigner requestSigner;

    public PrometheusRemoteWriteHttpRequestBuilder(
            String prometheusRemoteWriteUrl, PrometheusRequestSigner requestSigner) {
        this.prometheusRemoteWriteUrl = prometheusRemoteWriteUrl;
        this.requestSigner = requestSigner;
    }

    public SimpleHttpRequest buildHttpRequest(byte[] httpRequestBody) {
        Map<String, String> headers = new HashMap<>(FIXED_HEADER);
        if (requestSigner != null) {
            requestSigner.addSignatureHeaders(headers, httpRequestBody);
        }

        var builder =
                SimpleRequestBuilder.post()
                        .setUri(prometheusRemoteWriteUrl)
                        .setBody(httpRequestBody, CONTENT_TYPE);

        for (Map.Entry<String, String> header : headers.entrySet()) {
            builder.addHeader(header.getKey(), header.getValue());
        }

        return builder.build();
    }
}
