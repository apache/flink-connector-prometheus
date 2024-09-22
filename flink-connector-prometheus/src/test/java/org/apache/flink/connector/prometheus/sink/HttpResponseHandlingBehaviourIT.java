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

import org.apache.flink.connector.prometheus.sink.errorhandling.PrometheusSinkWriteException;
import org.apache.flink.connector.prometheus.sink.http.PrometheusAsyncHttpClientBuilder;
import org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics;
import org.apache.flink.connector.prometheus.sink.metrics.SinkMetricsCallback;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.status;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.apache.flink.connector.prometheus.sink.HttpResponseCallbackTestUtils.assertCallbackCompletedOnceWithException;
import static org.apache.flink.connector.prometheus.sink.HttpResponseCallbackTestUtils.assertCallbackCompletedOnceWithNoException;
import static org.apache.flink.connector.prometheus.sink.HttpResponseCallbackTestUtils.getRequestResult;
import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration.ON_HTTP_CLIENT_IO_FAIL_DEFAULT_BEHAVIOR;
import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration.ON_MAX_RETRY_EXCEEDED_DEFAULT_BEHAVIOR;
import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration.ON_PROMETHEUS_NON_RETRIABLE_ERROR_DEFAULT_BEHAVIOR;
import static org.awaitility.Awaitility.await;

/**
 * Test the http response handling behaviour with the full stack handling http client, retries and
 * error handling.
 *
 * <p>The full behaviour is determined by the combination of {@link HttpResponseCallback}, {@link
 * org.apache.flink.connector.prometheus.sink.http.RemoteWriteRetryStrategy}, {@link
 * PrometheusSinkConfiguration.RetryConfiguration}, {@link
 * org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseClassifier}, and {@link
 * PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration}.
 *
 * <p>The behaviour of the stack is tested sending a request through the http client and simulating
 * the response from Prometheus using a WireMock stub.
 */
@WireMockTest
public class HttpResponseHandlingBehaviourIT {

    private static final int TIME_SERIES_COUNT = 13;
    private static final long SAMPLE_COUNT = 42;

    private SinkMetricsCallback metricsCallback;

    @BeforeEach
    void setUp() {
        metricsCallback =
                new SinkMetricsCallback(
                        SinkMetrics.registerSinkMetrics(new UnregisteredMetricsGroup()));
    }

    private SimpleHttpRequest buildRequest(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException {
        return HttpTestUtils.buildPostRequest(HttpTestUtils.buildRequestUrl(wmRuntimeInfo));
    }

    private void serverWillRespond(ResponseDefinitionBuilder responseDefinition) {
        WireMock.stubFor(post("/remote_write").willReturn(responseDefinition));
    }

    private PrometheusAsyncHttpClientBuilder getHttpClientBuilder(int maxRetryCount) {
        return new PrometheusAsyncHttpClientBuilder(
                HttpTestUtils.fastRetryConfiguration(maxRetryCount));
    }

    private VerifyableResponseCallback getResponseCallback(
            SinkMetricsCallback metricsCallback,
            PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                    errorHandlingBehavior) {
        return new VerifyableResponseCallback(
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        getRequestResult(new ArrayList<>())));
    }

    @Test
    void shouldCompleteOn200Ok(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException, IOException {
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(1);

        // 200,OK: success
        int statusCode = 200;
        serverWillRespond(status(statusCode));

        // Default behaviors for all errors
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onMaxRetryExceeded(ON_MAX_RETRY_EXCEEDED_DEFAULT_BEHAVIOR)
                                .onHttpClientIOFail(ON_HTTP_CLIENT_IO_FAIL_DEFAULT_BEHAVIOR)
                                .onPrometheusNonRetriableError(
                                        ON_PROMETHEUS_NON_RETRIABLE_ERROR_DEFAULT_BEHAVIOR)
                                .build();

        VerifyableResponseCallback callback =
                getResponseCallback(metricsCallback, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metricsCallback)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the client execute only one request
                                verify(exactly(1), postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback is completed
                                assertCallbackCompletedOnceWithNoException(callback);
                            });
        }
    }

    @Test
    void shouldCompleteAfterRetryingOn500WhenDiscardAndContinueOnMaxRetryExceededIsSelected(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        int maxRetryCount = 2;
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(maxRetryCount);

        // 500,Server error is retriable for Prometheus remote-write
        int statusCode = 500;
        serverWillRespond(status(statusCode));

        // Discard and continue on max retry exceeded
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onMaxRetryExceeded(
                                        PrometheusSinkConfiguration.OnErrorBehavior
                                                .DISCARD_AND_CONTINUE)
                                .build();

        VerifyableResponseCallback callback =
                getResponseCallback(metricsCallback, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metricsCallback)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the http client retries for max retries + one initial
                                // attempt
                                verify(
                                        exactly(maxRetryCount + 1),
                                        postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback is completed
                                assertCallbackCompletedOnceWithNoException(callback);
                            });
        }
    }

    @Test
    void shouldRetryCompleteAndThrowExceptionOn500WhenFailOnMaxRetryExceededIsSelected(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        int maxRetryCount = 2;
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(maxRetryCount);

        // 500,Server error is retriable for Prometheus remote-write
        int statusCode = 500;
        serverWillRespond(status(statusCode));

        // Fail on max retry exceeded
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onMaxRetryExceeded(
                                        PrometheusSinkConfiguration.OnErrorBehavior.FAIL)
                                .build();

        VerifyableResponseCallback callback =
                getResponseCallback(metricsCallback, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metricsCallback)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the http client retries for max retries + one initial
                                // attempt
                                verify(
                                        exactly(maxRetryCount + 1),
                                        postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback was completed once with an exception
                                assertCallbackCompletedOnceWithException(
                                        PrometheusSinkWriteException.class, callback);
                            });
        }
    }

    @Test
    void shouldNotRetryAndCompleteOn404WhenDiscardAndContinueOnNonRetriableIsSelected(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(1);

        // 404,Not found is non-retriable for Prometheus remote-write
        int statusCode = 404;
        serverWillRespond(status(statusCode));

        // Discard and continue on non-retriable
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onPrometheusNonRetriableError(
                                        PrometheusSinkConfiguration.OnErrorBehavior
                                                .DISCARD_AND_CONTINUE)
                                .build();

        VerifyableResponseCallback callback =
                getResponseCallback(metricsCallback, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metricsCallback)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the client execute only one request
                                verify(exactly(1), postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback is completed
                                assertCallbackCompletedOnceWithNoException(callback);
                            });
        }
    }

    @Test
    void shouldNotRetryCompleteAndThrowExceptionOn304(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException, IOException {

        // 304, Not modified is just status code for which the behaviour is not
        // specified by Prometheus remote-write specs, and makes the http client
        // terminate successfully
        int statusCode = 304;
        serverWillRespond(status(statusCode));

        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(1);

        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .build();

        VerifyableResponseCallback callback =
                getResponseCallback(metricsCallback, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metricsCallback)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the client execute only one request
                                verify(exactly(1), postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback was completed once with an exception
                                assertCallbackCompletedOnceWithException(
                                        PrometheusSinkWriteException.class, callback);
                            });
        }
    }
}
