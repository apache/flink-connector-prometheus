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

import org.apache.flink.connector.prometheus.sink.http.RetryConfiguration;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.net.URIBuilder;

import java.net.URISyntaxException;

/** Utilities for WireMock integration tests. */
public class WireMockTestUtils {
    public static String buildRequestUrl(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException {
        return new URIBuilder(wmRuntimeInfo.getHttpBaseUrl())
                .setPath("/remote_write")
                .setPort(wmRuntimeInfo.getHttpPort())
                .build()
                .toString();
    }

    public static SimpleHttpRequest buildPostRequest(String requestUrl) {
        return SimpleRequestBuilder.post()
                .setUri(requestUrl)
                .setBody("N/A", ContentType.DEFAULT_BINARY)
                .build();
    }

    public static RetryConfiguration retryConfiguration(int maxRetryCount) {
        return RetryConfiguration.builder()
                .setInitialRetryDelayMS(1)
                .setMaxRetryDelayMS(1)
                .setMaxRetryCount(maxRetryCount)
                .build();
    }
}
