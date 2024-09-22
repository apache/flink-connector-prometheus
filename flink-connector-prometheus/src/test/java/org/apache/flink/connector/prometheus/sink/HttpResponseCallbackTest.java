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
import org.apache.flink.connector.prometheus.sink.metrics.VerifybleSinkMetricsCallback;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration.ON_HTTP_CLIENT_IO_FAIL_DEFAULT_BEHAVIOR;
import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration.ON_MAX_RETRY_EXCEEDED_DEFAULT_BEHAVIOR;
import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration.ON_PROMETHEUS_NON_RETRIABLE_ERROR_DEFAULT_BEHAVIOR;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpResponseCallbackTest {

    private static final int TIME_SERIES_COUNT = 17;
    private static final long SAMPLE_COUNT = 42;

    private InspectableMetricGroup metricGroup;
    private VerifybleSinkMetricsCallback metricsCallback;
    private List<Types.TimeSeries> reQueuedResults;
    Consumer<List<Types.TimeSeries>> requestResults;

    @BeforeEach
    void setUp() {
        metricGroup = new InspectableMetricGroup();
        metricsCallback = new VerifybleSinkMetricsCallback();
        reQueuedResults = new ArrayList<>();
        requestResults = HttpResponseCallbackTestUtils.getRequestResult(reQueuedResults);
    }

    @Test
    void shouldIncSuccessCountersOn200OK() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onMaxRetryExceeded(ON_MAX_RETRY_EXCEEDED_DEFAULT_BEHAVIOR)
                                .onHttpClientIOFail(ON_HTTP_CLIENT_IO_FAIL_DEFAULT_BEHAVIOR)
                                .onPrometheusNonRetriableError(
                                        ON_PROMETHEUS_NON_RETRIABLE_ERROR_DEFAULT_BEHAVIOR)
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_OK);

        callback.completed(httpResponse);

        // Verify only the expected metrics callback was called, once
        assertTrue(metricsCallback.verifyOnlySuccessfulWriteRequestsWasCalledOnce());

        // No time series is re-queued
        HttpResponseCallbackTestUtils.assertNoReQueuedResult(reQueuedResults);
    }

    @Test
    void shouldIncFailCountersOnCompletedWith404WhenDiscardAndContinueOnNonRetriableIsSelected() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onPrometheusNonRetriableError(
                                        PrometheusSinkConfiguration.OnErrorBehavior
                                                .DISCARD_AND_CONTINUE)
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_NOT_FOUND);

        callback.completed(httpResponse);

        // Verify only the expected metrics callback was called, once
        assertTrue(
                metricsCallback.verifyOnlyFailedWriteRequestsForNonRetriableErrorWasCalledOnce());

        // No time series is re-queued
        HttpResponseCallbackTestUtils.assertNoReQueuedResult(reQueuedResults);
    }

    @Test
    void shouldThrowExceptionsOnCompletedWith500WhenFailOnRetryExceededIsSelected() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onMaxRetryExceeded(
                                        PrometheusSinkConfiguration.OnErrorBehavior.FAIL)
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_SERVER_ERROR);

        assertThrows(
                PrometheusSinkWriteException.class,
                () -> {
                    callback.completed(httpResponse);
                });
    }

    @Test
    void shouldIncFailCountersOnCompletedWith500WhenDiscardAndContinueOnRetryExceededIsSelected() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onMaxRetryExceeded(
                                        PrometheusSinkConfiguration.OnErrorBehavior
                                                .DISCARD_AND_CONTINUE)
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_SERVER_ERROR);

        callback.completed(httpResponse);

        // Verify only the expected metric callback was called, once
        assertTrue(
                metricsCallback.verifyOnlyFailedWriteRequestsForRetryLimitExceededWasCalledOnce());

        // No time series is re-queued
        HttpResponseCallbackTestUtils.assertNoReQueuedResult(reQueuedResults);
    }

    @Test
    void shouldThrowExceptionOnCompletedWith100() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(100);

        assertThrows(
                PrometheusSinkWriteException.class,
                () -> {
                    callback.completed(httpResponse);
                });
    }

    @Test
    void shouldThrowExceptionOnFailedWhenFailOnHttpIOFailureIsSelected() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onHttpClientIOFail(
                                        PrometheusSinkConfiguration.OnErrorBehavior.FAIL)
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        Exception ex = new UnsupportedOperationException("Dummy exceptions");

        assertThrows(
                PrometheusSinkWriteException.class,
                () -> {
                    callback.failed(ex);
                });
    }

    @Test
    void shouldIncFailCountersOnFailedWhenDiscardAndContinueOnHttpIOFailureIsSelected() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .onHttpClientIOFail(
                                        PrometheusSinkConfiguration.OnErrorBehavior
                                                .DISCARD_AND_CONTINUE)
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        Exception ex = new UnsupportedOperationException("Dummy exceptions");

        callback.failed(ex);

        // Verify only the expected metric callback was called, once
        assertTrue(metricsCallback.verifyOnlyFailedWriteRequestsForHttpClientIoFailWasCalledOnce());

        // No time series is re-queued
        HttpResponseCallbackTestUtils.assertNoReQueuedResult(reQueuedResults);
    }

    @Test
    void shouldThrowExceptionOnCancelled() {
        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                errorHandlingBehavior =
                        PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                                .builder()
                                .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        requestResults);

        assertThrows(
                PrometheusSinkWriteException.class,
                () -> {
                    callback.cancelled();
                });
    }
}
