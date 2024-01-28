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

package org.apache.flink.connector.prometheus.sink.http;

import org.apache.flink.connector.prometheus.sink.SinkMetrics;
import org.apache.flink.connector.prometheus.sink.WireMockTestUtils;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.status;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.apache.flink.connector.prometheus.sink.http.HttpClientTestUtils.statusCodeAsserter;
import static org.awaitility.Awaitility.await;

/**
 * Test the stack of RemoteWriteRetryStrategy, RetryConfiguration and RemoteWriteResponseClassifier,
 * and PrometheusAsyncHttpClientBuilder.
 */
@WireMockTest
public class AsyncHttpClientRetryIT {

    private SinkMetrics dummySinkMetrics() {
        return SinkMetrics.registerSinkMetrics(
                UnregisteredMetricsGroup.createSinkWriterMetricGroup());
    }

    @Test
    public void shouldRetryOn500UpToRetryLimitThenSuccessfullyReturn(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        stubFor(post("/remote_write").willReturn(serverError()));

        SinkMetrics metrics = dummySinkMetrics();

        int retryLimit = 10;
        int expectedRequestCount = retryLimit + 1;
        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(
                        WireMockTestUtils.retryConfiguration(retryLimit));
        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
            SimpleHttpRequest request =
                    WireMockTestUtils.buildPostRequest(
                            WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));
            client.execute(request, statusCodeAsserter(HttpStatus.SC_SERVER_ERROR));

            await().untilAsserted(
                            () -> {
                                verify(
                                        exactly(expectedRequestCount),
                                        postRequestedFor(urlEqualTo("/remote_write")));
                            });
        }
    }

    @Test
    public void shouldRetryOn429UpToRetryLimitThenSuccessfullyReturn(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        stubFor(post("/remote_write").willReturn(status(HttpStatus.SC_TOO_MANY_REQUESTS)));
        SinkMetrics metrics = dummySinkMetrics();

        int retryLimit = 10;
        int expectedRequestCount = retryLimit + 1;
        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(
                        WireMockTestUtils.retryConfiguration(retryLimit));
        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
            SimpleHttpRequest request =
                    WireMockTestUtils.buildPostRequest(
                            WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));
            client.execute(request, statusCodeAsserter(HttpStatus.SC_TOO_MANY_REQUESTS));

            await().untilAsserted(
                            () -> {
                                verify(
                                        exactly(expectedRequestCount),
                                        postRequestedFor(urlEqualTo("/remote_write")));
                            });
        }
    }

    @Test
    public void shouldNotRetryOn404ThenSuccessfullyReturn(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException, IOException {
        stubFor(post("/remote_write").willReturn(notFound()));
        SinkMetrics metrics = dummySinkMetrics();

        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(WireMockTestUtils.retryConfiguration(2));

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
            SimpleHttpRequest request =
                    WireMockTestUtils.buildPostRequest(
                            WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));
            client.execute(request, statusCodeAsserter(HttpStatus.SC_NOT_FOUND));

            await().untilAsserted(
                            () -> {
                                verify(exactly(1), postRequestedFor(urlEqualTo("/remote_write")));
                            });
        }
    }

    @Test
    void shouldNotRetryOn200OkThenSuccessfullyReturn(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException, IOException {
        stubFor(post("/remote_write").willReturn(ok()));
        SinkMetrics metrics = dummySinkMetrics();

        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(WireMockTestUtils.retryConfiguration(2));

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
            SimpleHttpRequest request =
                    WireMockTestUtils.buildPostRequest(
                            WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));
            client.execute(request, statusCodeAsserter(HttpStatus.SC_OK));

            await().untilAsserted(
                            () -> {
                                verify(exactly(1), postRequestedFor(urlEqualTo("/remote_write")));
                            });
        }
    }
}
