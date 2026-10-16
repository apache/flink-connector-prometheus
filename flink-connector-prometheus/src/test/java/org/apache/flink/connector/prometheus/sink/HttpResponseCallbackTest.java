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

import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration;
import org.apache.flink.connector.prometheus.sink.errorhandling.PrometheusSinkWriteException;
import org.apache.flink.connector.prometheus.sink.metrics.VerifybleSinkMetricsCallback;

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpResponseCallbackTest {

    private static final int TIME_SERIES_COUNT = 17;
    private static final long SAMPLE_COUNT = 42;

    private VerifybleSinkMetricsCallback metricsCallback;
    private HttpResponseCallbackTestUtils.CapturingResultHandler resultHandler;

    @BeforeEach
    void setUp() {
        metricsCallback = new VerifybleSinkMetricsCallback();
        resultHandler = new HttpResponseCallbackTestUtils.CapturingResultHandler();
    }

    @Test
    void shouldIncSuccessCountersOn200OK() {
        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        SinkWriterErrorHandlingBehaviorConfiguration.DEFAULT_BEHAVIORS,
                        resultHandler);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_OK);

        callback.completed(httpResponse);

        // Verify only the expected metrics callback was called, once
        assertTrue(metricsCallback.verifyOnlySuccessfulWriteRequestsWasCalledOnce());

        // ResultHandler.complete() was called
        assertTrue(resultHandler.isCompleted());
        assertFalse(resultHandler.isCompletedExceptionally());
    }

    @Test
    void shouldIncFailCountersOnCompletedWith400WhenDiscardAndContinueOnNonRetryableIsSelected() {
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onPrometheusNonRetryableError(
                                PrometheusSinkConfiguration.OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        resultHandler);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_BAD_REQUEST);

        callback.completed(httpResponse);

        // Verify only the expected metrics callback was called, once
        assertTrue(
                metricsCallback.verifyOnlyFailedWriteRequestsForNonRetryableErrorWasCalledOnce());

        // ResultHandler.complete() was called (discard and continue)
        assertTrue(resultHandler.isCompleted());
        assertFalse(resultHandler.isCompletedExceptionally());
    }

    @Test
    void shouldSignalExceptionOnCompletedWith500WhenFailOnRetryExceededIsSelected() {
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(PrometheusSinkConfiguration.OnErrorBehavior.FAIL)
                        .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        resultHandler);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_SERVER_ERROR);

        callback.completed(httpResponse);

        // Exception should be signaled via resultHandler.completeExceptionally()
        assertTrue(resultHandler.isCompletedExceptionally());
        assertInstanceOf(PrometheusSinkWriteException.class, resultHandler.getException());
    }

    @Test
    void shouldIncFailCountersOnCompletedWith500WhenDiscardAndContinueOnRetryExceededIsSelected() {
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(
                                PrometheusSinkConfiguration.OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        resultHandler);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(HttpStatus.SC_SERVER_ERROR);

        callback.completed(httpResponse);

        // Verify only the expected metric callback was called, once
        assertTrue(
                metricsCallback.verifyOnlyFailedWriteRequestsForRetryLimitExceededWasCalledOnce());

        // ResultHandler.complete() was called (discard and continue)
        assertTrue(resultHandler.isCompleted());
        assertFalse(resultHandler.isCompletedExceptionally());
    }

    @Test
    void shouldSignalExceptionOnCompletedWith100() {
        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        SinkWriterErrorHandlingBehaviorConfiguration.DEFAULT_BEHAVIORS,
                        resultHandler);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(100);

        callback.completed(httpResponse);

        // Exception should be signaled via resultHandler.completeExceptionally()
        assertTrue(resultHandler.isCompletedExceptionally());
        assertInstanceOf(PrometheusSinkWriteException.class, resultHandler.getException());
    }

    @Test
    void shouldSignalExceptionOnCompletedWith403() {
        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        SinkWriterErrorHandlingBehaviorConfiguration.DEFAULT_BEHAVIORS,
                        resultHandler);

        SimpleHttpResponse httpResponse = new SimpleHttpResponse(403);

        callback.completed(httpResponse);

        // Exception should be signaled via resultHandler.completeExceptionally()
        assertTrue(resultHandler.isCompletedExceptionally());
        assertInstanceOf(PrometheusSinkWriteException.class, resultHandler.getException());
    }

    @Test
    void shouldSignalExceptionOnCancelled() {
        SinkWriterErrorHandlingBehaviorConfiguration errorHandlingBehavior =
                SinkWriterErrorHandlingBehaviorConfiguration.builder().build();

        HttpResponseCallback callback =
                new HttpResponseCallback(
                        TIME_SERIES_COUNT,
                        SAMPLE_COUNT,
                        metricsCallback,
                        errorHandlingBehavior,
                        resultHandler);

        callback.cancelled();

        // Exception should be signaled via resultHandler.completeExceptionally()
        assertTrue(resultHandler.isCompletedExceptionally());
        assertInstanceOf(PrometheusSinkWriteException.class, resultHandler.getException());
    }
}
