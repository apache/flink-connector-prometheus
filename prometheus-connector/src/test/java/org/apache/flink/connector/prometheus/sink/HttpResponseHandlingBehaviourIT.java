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
import org.apache.flink.connector.prometheus.sink.errorhandling.SinkWriterErrorHandlingBehaviorConfiguration;
import org.apache.flink.connector.prometheus.sink.http.PrometheusAsyncHttpClientBuilder;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.apache.flink.connector.prometheus.sink.InspectableMetricGroupAssertions.assertCounterCount;
import static org.apache.flink.connector.prometheus.sink.InspectableMetricGroupAssertions.assertCounterWasNotIncremented;
import static org.apache.flink.connector.prometheus.sink.SinkMetrics.SinkCounter.NUM_SAMPLES_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkMetrics.SinkCounter.NUM_SAMPLES_NON_RETRIABLE_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkMetrics.SinkCounter.NUM_SAMPLES_OUT;
import static org.apache.flink.connector.prometheus.sink.SinkMetrics.SinkCounter.NUM_SAMPLES_RETRY_LIMIT_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkMetrics.SinkCounter.NUM_WRITE_REQUESTS_OUT;
import static org.apache.flink.connector.prometheus.sink.SinkMetrics.SinkCounter.NUM_WRITE_REQUESTS_PERMANENTLY_FAILED;
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
 * <p>The behaviour of the stack is tested sending a http request through the http client and
 * simulating the response from Prometheus with a WireMock stub.
 */
@WireMockTest
public class HttpResponseHandlingBehaviourIT {

    private static final int TIME_SERIES_COUNT = 13;
    private static final long SAMPLE_COUNT = 42;

    private Consumer<List<Types.TimeSeries>> getRequestResult(
            List<Types.TimeSeries> emittedResults) {
        return emittedResults::addAll;
    }

    private SimpleHttpRequest buildRequest(WireMockRuntimeInfo wmRuntimeInfo)
            throws URISyntaxException {
        return WireMockTestUtils.buildPostRequest(WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));
    }

    private void serverWillRespond(ResponseDefinitionBuilder responseDefinition) {
        WireMock.stubFor(post("/remote_write").willReturn(responseDefinition));
    }

    private PrometheusAsyncHttpClientBuilder getHttpClientBuilder(int maxRetryCount) {
        return new PrometheusAsyncHttpClientBuilder(
                WireMockTestUtils.fastRetryConfiguration(maxRetryCount));
    }

    private VerifyableResponseCallback getResponseCallback(
            SinkMetrics metrics,
            SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior,
            List<Types.TimeSeries> requeuedResults) {
        return new VerifyableResponseCallback(
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metrics,
                        errorHandlingBehavior,
                        getRequestResult(requeuedResults)));
    }

    @Test
    void shouldCompleteAndIncrementSamplesOutAndWriteRequestsOn200Ok(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {

        InspectableMetricGroup metricGroup = new InspectableMetricGroup();
        SinkMetrics metrics = SinkMetrics.registerSinkMetrics(metricGroup);

        int maxRetryCount = 1;
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(maxRetryCount);

        // Always fail on any error
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(OnErrorBehavior.FAIL)
                        .onHttpClientIOFail(OnErrorBehavior.FAIL)
                        .onPrometheusNonRetriableError(OnErrorBehavior.FAIL)
                        .build();

        List<Types.TimeSeries> requeuedResults = new ArrayList<>();
        VerifyableResponseCallback callback =
                getResponseCallback(metrics, errorHandlingBehavior, requeuedResults);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);
        serverWillRespond(ok());

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the client execute only one request
                                WireMock.verify(
                                        WireMock.exactly(1),
                                        postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback is completed
                                assertCallbackCompletedOnce(callback);

                                // Verify no result was requeued
                                assertNoReQueuedResult(requeuedResults);

                                // Verify counters have been incremented to the expected values
                                assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_OUT);
                                assertCounterCount(1, metricGroup, NUM_WRITE_REQUESTS_OUT);

                                // Verify other counters have not been incremented
                                assertCounterWasNotIncremented(metricGroup, NUM_SAMPLES_DROPPED);
                                assertCounterWasNotIncremented(
                                        metricGroup, NUM_SAMPLES_RETRY_LIMIT_DROPPED);
                                assertCounterWasNotIncremented(
                                        metricGroup, NUM_SAMPLES_NON_RETRIABLE_DROPPED);
                                assertCounterWasNotIncremented(
                                        metricGroup, NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);
                            });
        }
    }

    @Test
    void
            shouldRetryAndIncrementSamplesDroppedAndRequestFailedAfterRetryingOn500WhenDiscardOnMaxRetryExceededIsSelected(
                    WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {

        InspectableMetricGroup metricGroup = new InspectableMetricGroup();
        SinkMetrics metrics = SinkMetrics.registerSinkMetrics(metricGroup);

        int maxRetryCount = 2;
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(maxRetryCount);

        // Discard on max retry exceeded, fail on any other error
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .onPrometheusNonRetriableError(OnErrorBehavior.FAIL)
                        .onHttpClientIOFail(OnErrorBehavior.FAIL)
                        .build();

        List<Types.TimeSeries> requeuedResults = new ArrayList<>();
        VerifyableResponseCallback callback =
                getResponseCallback(metrics, errorHandlingBehavior, requeuedResults);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);
        serverWillRespond(serverError());

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the http client retries for max retries + one initial
                                // attempt
                                WireMock.verify(
                                        WireMock.exactly(maxRetryCount + 1),
                                        postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback is completed
                                assertCallbackCompletedOnce(callback);

                                // Verify no result was re-queued
                                assertNoReQueuedResult(requeuedResults);

                                // Verify counters have been incremented to the expected values
                                assertCounterCount(
                                        SAMPLE_COUNT, metricGroup, NUM_SAMPLES_RETRY_LIMIT_DROPPED);
                                assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_DROPPED);
                                assertCounterCount(
                                        1, metricGroup, NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);

                                // Verify other counters have not been incremented
                                assertCounterWasNotIncremented(metricGroup, NUM_SAMPLES_OUT);
                                assertCounterWasNotIncremented(
                                        metricGroup, NUM_SAMPLES_NON_RETRIABLE_DROPPED);
                                assertCounterWasNotIncremented(metricGroup, NUM_WRITE_REQUESTS_OUT);
                            });
        }
    }

    @Test
    void
            shouldNotRetryAndIncrementSamplesDroppedAndRequestFailedAfterRetryingOn404WhenDiscardOnNonRetriableIsSelected(
                    WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {

        InspectableMetricGroup metricGroup = new InspectableMetricGroup();
        SinkMetrics metrics = SinkMetrics.registerSinkMetrics(metricGroup);

        int maxRetryCount = 1;
        PrometheusAsyncHttpClientBuilder clientBuilder = getHttpClientBuilder(maxRetryCount);

        // Discard on non retryable, fail on any other error
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(OnErrorBehavior.FAIL)
                        .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .onHttpClientIOFail(OnErrorBehavior.FAIL)
                        .build();

        List<Types.TimeSeries> requeuedResults = new ArrayList<>();
        VerifyableResponseCallback callback =
                getResponseCallback(metrics, errorHandlingBehavior, requeuedResults);

        SimpleHttpRequest request = buildRequest(wmRuntimeInfo);
        serverWillRespond(notFound());

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(metrics)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the client execute only one request
                                WireMock.verify(
                                        WireMock.exactly(1),
                                        postRequestedFor(urlEqualTo("/remote_write")));

                                // Verify the callback is completed
                                assertCallbackCompletedOnce(callback);

                                // Verify no result was requeued
                                assertNoReQueuedResult(requeuedResults);

                                // Verify counters have been incremented to the expected values
                                assertCounterCount(
                                        SAMPLE_COUNT,
                                        metricGroup,
                                        NUM_SAMPLES_NON_RETRIABLE_DROPPED);
                                assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_DROPPED);
                                assertCounterCount(
                                        1, metricGroup, NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);

                                // Verify other counters have not been incremented
                                assertCounterWasNotIncremented(metricGroup, NUM_SAMPLES_OUT);
                                assertCounterWasNotIncremented(metricGroup, NUM_WRITE_REQUESTS_OUT);
                                assertCounterWasNotIncremented(
                                        metricGroup, NUM_SAMPLES_RETRY_LIMIT_DROPPED);
                            });
        }
    }

    // TODO test more behaviours

    private static void assertNoReQueuedResult(List<Types.TimeSeries> emittedResults) {
        Assertions.assertTrue(
                emittedResults.isEmpty(),
                emittedResults.size() + " results were re-queued, but none was expected");
    }

    private static void assertCallbackCompletedOnce(VerifyableResponseCallback callback) {
        int actualCompletionCount = callback.getCompletedResponsesCount();
        Assertions.assertEquals(
                1,
                actualCompletionCount,
                "The callback was completed "
                        + actualCompletionCount
                        + " times, but once was expected");
    }
}
