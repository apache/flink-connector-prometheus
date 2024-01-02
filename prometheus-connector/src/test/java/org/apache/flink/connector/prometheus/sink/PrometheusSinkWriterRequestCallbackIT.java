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

import org.apache.flink.connector.prometheus.sink.http.PrometheusAsyncHttpClientBuilder;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_NON_RETRIABLE_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_OUT;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_RETRY_LIMIT_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_WRITE_REQUESTS_OUT;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_WRITE_REQUESTS_PERMANENTLY_FAILED;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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

    @Test
    void shouldCompleteAndIncrementSamplesOutAndWriteRequestsOn200Ok(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        SinkCounters counters = mock(SinkCounters.class);
        Consumer<List<Types.TimeSeries>> requestResult = mock(Consumer.class);
        ArgumentCaptor<SinkCounters.SinkCounter> captorCounterInc =
                ArgumentCaptor.forClass(SinkCounters.SinkCounter.class);
        ArgumentCaptor<SinkCounters.SinkCounter> captorCounterIncValue =
                ArgumentCaptor.forClass(SinkCounters.SinkCounter.class);

        WireMock.stubFor(post("/remote_write").willReturn(ok()));

        int maxRetryCount = 1;
        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(
                        WireMockTestUtils.retryConfiguration(maxRetryCount));
        SimpleHttpRequest request =
                WireMockTestUtils.buildPostRequest(
                        WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));

        PrometheusSinkWriter.ResponseCallback callback =
                spy(
                        new PrometheusSinkWriter.ResponseCallback(
                                TIME_SERIES_COUNT, SAMPLE_COUNT, counters, requestResult));

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(counters)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the client execute only one request
                                WireMock.verify(
                                        WireMock.exactly(1),
                                        postRequestedFor(urlEqualTo("/remote_write")));

                                // Check the callback is called and no entry is re-queued
                                verify(callback).completed(any());
                                verify(requestResult).accept(eq(Collections.emptyList()));

                                // Capture the counter inc calls...
                                verify(counters, atLeastOnce())
                                        .inc(captorCounterIncValue.capture(), eq(SAMPLE_COUNT));
                                verify(counters, atLeastOnce()).inc(captorCounterInc.capture());

                                // Check expected counters are incremented
                                assertCounterWasIncremented(NUM_SAMPLES_OUT, captorCounterIncValue);
                                assertCounterWasIncremented(
                                        NUM_WRITE_REQUESTS_OUT, captorCounterInc);

                                // Check expected counters are not incremented
                                assertCounterWasNotIncremented(
                                        NUM_SAMPLES_DROPPED, captorCounterIncValue);
                                assertCounterWasNotIncremented(
                                        NUM_SAMPLES_RETRY_LIMIT_DROPPED, captorCounterIncValue);
                                assertCounterWasNotIncremented(
                                        NUM_SAMPLES_NON_RETRIABLE_DROPPED, captorCounterIncValue);
                                assertCounterWasNotIncremented(
                                        NUM_WRITE_REQUESTS_PERMANENTLY_FAILED,
                                        captorCounterIncValue);
                                assertCounterWasNotIncremented(
                                        NUM_WRITE_REQUESTS_PERMANENTLY_FAILED, captorCounterInc);
                            });
        }
    }

    @Test
    void shouldCompleteAndIncrementSamplesDroppedAndRequestFailedAfterRetryingOn500(
            WireMockRuntimeInfo wmRuntimeInfo) throws URISyntaxException, IOException {
        SinkCounters counters = mock(SinkCounters.class);
        Consumer<List<Types.TimeSeries>> requestResult = mock(Consumer.class);
        ArgumentCaptor<SinkCounters.SinkCounter> captorCounterInc =
                ArgumentCaptor.forClass(SinkCounters.SinkCounter.class);
        ArgumentCaptor<SinkCounters.SinkCounter> captorCounterIncValue =
                ArgumentCaptor.forClass(SinkCounters.SinkCounter.class);

        WireMock.stubFor(post("/remote_write").willReturn(serverError()));

        int maxRetryCount = 2;
        PrometheusAsyncHttpClientBuilder clientBuilder =
                new PrometheusAsyncHttpClientBuilder(
                        WireMockTestUtils.retryConfiguration(maxRetryCount));
        SimpleHttpRequest request =
                WireMockTestUtils.buildPostRequest(
                        WireMockTestUtils.buildRequestUrl(wmRuntimeInfo));

        PrometheusSinkWriter.ResponseCallback callback =
                spy(
                        new PrometheusSinkWriter.ResponseCallback(
                                TIME_SERIES_COUNT, SAMPLE_COUNT, counters, requestResult));

        try (CloseableHttpAsyncClient client = clientBuilder.buildAndStartClient(counters)) {
            client.execute(request, callback);

            await().untilAsserted(
                            () -> {
                                // Check the client retries
                                WireMock.verify(
                                        WireMock.exactly(maxRetryCount + 1),
                                        postRequestedFor(
                                                urlEqualTo("/remote_write"))); // max retries + 1
                                // initial attempt

                                // Check the callback is called and no entry is re-queued
                                verify(callback).completed(any());
                                verify(requestResult).accept(eq(Collections.emptyList()));

                                // Capture the counter inc calls...
                                verify(counters, atLeastOnce())
                                        .inc(captorCounterIncValue.capture(), eq(SAMPLE_COUNT));
                                verify(counters, atLeastOnce()).inc(captorCounterInc.capture());

                                // Check expected counters are incremented
                                assertCounterWasIncremented(
                                        NUM_SAMPLES_RETRY_LIMIT_DROPPED, captorCounterIncValue);
                                assertCounterWasIncremented(
                                        NUM_SAMPLES_DROPPED, captorCounterIncValue);
                                assertCounterWasIncremented(
                                        NUM_WRITE_REQUESTS_PERMANENTLY_FAILED, captorCounterInc);

                                // Check other counters are not incremented
                                assertCounterWasNotIncremented(
                                        NUM_SAMPLES_OUT, captorCounterIncValue);
                                assertCounterWasNotIncremented(
                                        NUM_SAMPLES_NON_RETRIABLE_DROPPED, captorCounterIncValue);
                                assertCounterWasNotIncremented(
                                        NUM_WRITE_REQUESTS_OUT, captorCounterInc);
                            });
        }
    }

    // TODO test more behaviours

    private static void assertCounterWasIncremented(
            SinkCounters.SinkCounter expectedCounter,
            ArgumentCaptor<SinkCounters.SinkCounter> counterTypeArg) {
        assertTrue(
                counterTypeArg.getAllValues().contains(expectedCounter),
                expectedCounter.getMetricName() + " should be incremented");
    }

    private static void assertCounterWasNotIncremented(
            SinkCounters.SinkCounter expectedCounter,
            ArgumentCaptor<SinkCounters.SinkCounter> counterTypeArg) {
        assertFalse(
                counterTypeArg.getAllValues().contains(expectedCounter),
                expectedCounter.getMetricName() + " should NOT be incremented");
    }
}
