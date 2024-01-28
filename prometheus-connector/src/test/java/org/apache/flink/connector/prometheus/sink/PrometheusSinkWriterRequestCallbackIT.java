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
 * Test the stack of RemoteWriteRetryStrategy, RetryConfiguration and RemoteWriteResponseClassifier,
 * PrometheusAsyncHttpClientBuilder, and PrometheusSinkWriter.ResponseCallback.
 *
 * <p>Test the callback method is called with the expected parameters. Test the correct counters are
 * incremented. Test no request is re-queued.
 */
@WireMockTest
public class PrometheusSinkWriterRequestCallbackIT {

    private static final int TIME_SERIES_COUNT = 13;
    private static final long SAMPLE_COUNT = 42;

    private Consumer<List<Types.TimeSeries>> requestResult(List<Types.TimeSeries> emittedResults) {
        return emittedResults::addAll;
    }

    @Test
    void shouldCompleteAndIncrementSamplesOutAndWriteRequestsOn200Ok(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {

        InspectableMetricGroup metricGroup = new InspectableMetricGroup();
        SinkMetrics metrics = SinkMetrics.registerSinkMetrics(metricGroup);

        List<Types.TimeSeries> requeuedResults = new ArrayList<>();
        Consumer<List<Types.TimeSeries>> requestResult = requestResult(requeuedResults);

        WireMock.stubFor(post("/remote_write").willReturn(ok()));

        int maxRetryCount = 1;
        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(
                        WireMockTestUtils.retryConfiguration(maxRetryCount));
        SimpleHttpRequest request =
                WireMockTestUtils.buildPostRequest(
                        WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));

        VerifyableResponseCallback callback =
                new VerifyableResponseCallback(
                        new PrometheusSinkWriter.ResponseCallback(
                                TIME_SERIES_COUNT,
                                SAMPLE_COUNT,
                                metrics,
                                SinkWriterErrorHandlingBehaviorConfiguration.DEFAULT_BEHAVIORS,
                                requestResult));

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
    void shouldCompleteAndIncrementSamplesDroppedAndRequestFailedAfterRetryingOn500(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {

        SinkWriterErrorHandlingBehaviorConfiguration discardOnNonRetriableError =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .build();

        InspectableMetricGroup metricGroup = new InspectableMetricGroup();
        SinkMetrics metrics = SinkMetrics.registerSinkMetrics(metricGroup);

        List<Types.TimeSeries> requeuedResults = new ArrayList<>();
        Consumer<List<Types.TimeSeries>> requestResult = requestResult(requeuedResults);

        WireMock.stubFor(post("/remote_write").willReturn(serverError()));

        int maxRetryCount = 2;
        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(
                        WireMockTestUtils.retryConfiguration(maxRetryCount));
        SimpleHttpRequest request =
                WireMockTestUtils.buildPostRequest(
                        WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));

        VerifyableResponseCallback callback =
                new VerifyableResponseCallback(
                        new PrometheusSinkWriter.ResponseCallback(
                                TIME_SERIES_COUNT,
                                SAMPLE_COUNT,
                                metrics,
                                discardOnNonRetriableError,
                                requestResult));

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
