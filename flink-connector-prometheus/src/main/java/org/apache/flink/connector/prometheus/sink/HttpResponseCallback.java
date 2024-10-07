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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.OnErrorBehavior;
import org.apache.flink.connector.prometheus.sink.errorhandling.PrometheusSinkWriteException;
import org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType;
import org.apache.flink.connector.prometheus.sink.metrics.SinkMetricsCallback;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseClassifier.classify;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.FATAL_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.NON_RETRIABLE_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.RETRIABLE_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.SUCCESS;

/**
 * Callback handling the outcome of the http async request.
 *
 * <p>This class implements the error handling behaviour, based on the configuration in {@link
 * PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration}. Depending on the
 * condition, the sink may throw an exception and cause the job to fail, or log the condition to
 * WARN, increment the counters and continue with the next request.
 *
 * <p>In any case, every write-request either entirely succeed or fail. Partial failures are not
 * handled.
 *
 * <p>In no condition a write-request is re-queued for the AsyncSink to reprocess: this would cause
 * out of order writes that would be rejected by Prometheus.
 *
 * <p>Note that the http async client retries, based on the configured retry policy. The callback is
 * called with an outcome of *completed* either when the request has succeeded or the max retry
 * limit has been exceeded. It is responsibility of the callback distinguishing between these
 * conditions.
 *
 * <p>Also, when an exception is thrown after the request is *completed*, for the http client point
 * of view (i.e. in the {@link #completed(SimpleHttpResponse)} callback method), it does not
 * directly cause the writer to fail until it is intercepted further up the client stack, by the
 * {@link org.apache.flink.connector.prometheus.sink.http.RethrowingIOSessionListener}.
 */
@Internal
class HttpResponseCallback implements FutureCallback<SimpleHttpResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpResponseCallback.class);

    private final int timeSeriesCount;
    private final long sampleCount;
    private final Consumer<List<Types.TimeSeries>> reQueuedResult;
    private final SinkMetricsCallback metricsCallback;
    private final PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
            errorHandlingBehaviorConfig;

    public HttpResponseCallback(
            int timeSeriesCount,
            long sampleCount,
            SinkMetricsCallback metricsCallback,
            PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration
                    errorHandlingBehaviorConfig,
            Consumer<List<Types.TimeSeries>> reQueuedResult) {
        this.timeSeriesCount = timeSeriesCount;
        this.sampleCount = sampleCount;
        this.reQueuedResult = reQueuedResult;
        this.metricsCallback = metricsCallback;
        this.errorHandlingBehaviorConfig = errorHandlingBehaviorConfig;
    }

    /**
     * The completed outcome is invoked when the http client successfully receives a response,
     * regardless of the status code.
     *
     * <p>This method classifies the responses using {@link
     * org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseClassifier} and implements
     * the behaviour expected by the Remote-Write specifications. If the response is classified as
     * an error, the behaviour is determined by the error handling configuration.
     */
    @Override
    public void completed(SimpleHttpResponse response) {
        // Never re-queue requests
        reQueuedResult.accept(Collections.emptyList());

        RemoteWriteResponseType responseType = classify(response);
        switch (responseType) {
            case SUCCESS: // Data successfully written
                // Increment successful writes counts
                metricsCallback.onSuccessfulWriteRequest(sampleCount);
                LOG.debug(
                        "{},{} - successfully posted {} time-series, containing {} samples",
                        response.getCode(),
                        response.getReasonPhrase(),
                        timeSeriesCount,
                        sampleCount);
                break;

            case FATAL_ERROR: // Response is a fatal error
                // Throw an exception regardless of configured behaviour
                logErrorAndThrow(
                        new PrometheusSinkWriteException(
                                "Fatal error response from Prometheus",
                                response.getCode(),
                                response.getReasonPhrase(),
                                timeSeriesCount,
                                sampleCount,
                                response.getBodyText()));
                break;

            case NON_RETRIABLE_ERROR: // Response is a non-retriable error.
                // If behavior is FAIL, throw an exception
                if (errorHandlingBehaviorConfig.getOnPrometheusNonRetriableError()
                        == OnErrorBehavior.FAIL) {
                    logErrorAndThrow(
                            new PrometheusSinkWriteException(
                                    "Non-retriable error response from Prometheus",
                                    response.getCode(),
                                    response.getReasonPhrase(),
                                    timeSeriesCount,
                                    sampleCount,
                                    response.getBodyText()));
                }

                // Otherwise (DISCARD_AND_CONTINUE), increment discarded data counts & log WARN
                metricsCallback.onFailedWriteRequestForNonRetriableError(sampleCount);
                LOG.warn(
                        "{},{} {} (discarded {} time-series, containing {} samples)",
                        response.getCode(),
                        response.getReasonPhrase(),
                        response.getBodyText(),
                        timeSeriesCount,
                        sampleCount);
                break;

            case RETRIABLE_ERROR: // Retry limit exceeded on retriable error
                // If behavior is FAIL, throw an exception
                if (errorHandlingBehaviorConfig.getOnMaxRetryExceeded() == OnErrorBehavior.FAIL) {
                    logErrorAndThrow(
                            new PrometheusSinkWriteException(
                                    "Max retry limit exceeded on retriable error",
                                    response.getCode(),
                                    response.getReasonPhrase(),
                                    timeSeriesCount,
                                    sampleCount,
                                    response.getBodyText()));
                }

                // Otherwise (DISCARD_AND_CONTINUE), increment discarded data counts & log WARN
                metricsCallback.onFailedWriteRequestForRetryLimitExceeded(sampleCount);
                LOG.warn(
                        "{},{} {} (after retry limit reached, discarded {} time-series, containing {} samples)",
                        response.getCode(),
                        response.getReasonPhrase(),
                        response.getBodyText(),
                        timeSeriesCount,
                        sampleCount);
                break;

            default: // Unexpected/unhandled response outcome
                // Always fail
                logErrorAndThrow(
                        new PrometheusSinkWriteException(
                                "Unexpected status code returned from the remote-write endpoint",
                                response.getCode(),
                                response.getReasonPhrase(),
                                timeSeriesCount,
                                sampleCount,
                                response.getBodyText()));
        }
    }

    /**
     * Exception reported by the http client (e.g. I/O failure). Always throw an exception.
     *
     * @param ex exception reported by the http client
     */
    @Override
    public void failed(Exception ex) {
        throw new PrometheusSinkWriteException("Http client failure", ex);
    }

    /** The async http client was cancelled. Always throw an exception. */
    @Override
    public void cancelled() {
        // When the async http client is cancelled, the sink should always throw an exception
        throw new PrometheusSinkWriteException("Write request execution cancelled");
    }

    /** Log the exception at ERROR and rethrow. */
    private void logErrorAndThrow(PrometheusSinkWriteException ex) {
        LOG.error("Error condition detected by the http response callback (on complete)", ex);
        throw ex;
    }
}
