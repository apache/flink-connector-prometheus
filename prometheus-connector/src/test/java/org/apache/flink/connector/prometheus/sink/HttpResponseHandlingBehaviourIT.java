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

import org.apache.flink.connector.prometheus.sink.errorhandling.OnErrorBehavior;
import org.apache.flink.connector.prometheus.sink.errorhandling.PrometheusSinkWriteException;
import org.apache.flink.connector.prometheus.sink.errorhandling.SinkWriterErrorHandlingBehaviorConfiguration;
import org.apache.flink.connector.prometheus.sink.http.PrometheusAsyncHttpClientBuilder;
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
import static org.awaitility.Awaitility.await;

/**
 * Test the http response handling behaviour with the full stack handling http client, retries and
 * error handling.
 *
 * <p>The full behaviour is determined by the combination of {@link HttpResponseCallback}, {@link
 * org.apache.flink.connector.prometheus.sink.http.RemoteWriteRetryStrategy}, {@link
 * org.apache.flink.connector.prometheus.sink.http.RetryConfiguration}, {@link
 * org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseClassifier}, and {@link
 * SinkWriterErrorHandlingBehaviorConfiguration}.
 *
 * <p>The behaviour of the stack is tested sending a request through the http client and simulating
 * the response from Prometheus using a WireMock stub.
 */
@WireMockTest
public class HttpResponseHandlingBehaviourIT {

    private static final int TIME_SERIES_COUNT = 13;
    private static final long SAMPLE_COUNT = 42;

    private SinkMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = SinkMetrics.registerSinkMetrics(new UnregisteredMetricsGroup());
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
            SinkMetrics metrics,
            SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior) {
        return new VerifyableResponseCallback(
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metrics,
                        errorHandlingBehavior,
                        getRequestResult(new ArrayList<>())));
    }

    @Test
    void shouldCompleteOn200Ok(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException, IOException {
        int statusCode = 200;
        serverWillRespond(status(statusCode));

        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(1);

        // Fail on any error
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(OnErrorBehavior.FAIL)
                        .onHttpClientIOFail(OnErrorBehavior.FAIL)
                        .onPrometheusNonRetriableError(OnErrorBehavior.FAIL)
                        .build();

        VerifyableResponseCallback callback = getResponseCallback(metrics, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
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
        int statusCode = 500; // 500,Server error is retriable for Prometheus remote-write
        serverWillRespond(status(statusCode));

        int maxRetryCount = 2;
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(maxRetryCount);

        // Discard and continue on max retry exceeded
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .build();

        VerifyableResponseCallback callback = getResponseCallback(metrics, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
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
        int statusCode = 500; // 500,Server error is retriable for Prometheus remote-write
        serverWillRespond(status(statusCode));

        int maxRetryCount = 2;
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(maxRetryCount);

        // Fail on max retry exceeded
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(OnErrorBehavior.FAIL)
                        .build();

        VerifyableResponseCallback callback = getResponseCallback(metrics, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
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
        int statusCode = 404; // 404,Not found is non-retriable for Prometheus remote-write
        serverWillRespond(status(statusCode));

        // Discard and continue on non-retriable
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .build();

        VerifyableResponseCallback callback = getResponseCallback(metrics, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
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
    void shouldNotRetryAndCompleteAndThrowExceptionOn404WhenFailOnNonRetriableIsSelected(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(1);
        int statusCode = 404; // 404,Not found is non-retriable for Prometheus remote-write
        serverWillRespond(status(statusCode));

        // Fail on non-retriable
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onPrometheusNonRetriableError(OnErrorBehavior.FAIL)
                        .build();

        VerifyableResponseCallback callback = getResponseCallback(metrics, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
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

    @Test
    void shouldNotRetryCompleteAndThrowExceptionOn304(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException, IOException {
        int statusCode =
                304; // 304, Not modified is just status code for which the behaviour is not
        // specified by Prometheus remote-write specs, and makes the http client
        // terminate successfully
        serverWillRespond(status(statusCode));

        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(1);

        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder().build();

        VerifyableResponseCallback callback = getResponseCallback(metrics, errorHandlingBehavior);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
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
