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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration;
import org.apache.flink.connector.prometheus.sink.metrics.SinkMetricsCallback;

import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseClassifier.classify;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.RETRYABLE_ERROR;

/**
 * Retry strategy for the http client.
 *
 * <p>Based on the http status code returned or the exception thrown, this strategy either retries
 * with an exponential backoff strategy or immediately fail.
 *
 * <p>Response status codes are classified as retryable or non-retryable using {@link
 * RemoteWriteResponseClassifier}.
 *
 * <p>All {@link IOException} are considered retryable, except for {@link InterruptedIOException},
 * {@link UnknownHostException}, {@link ConnectException}, {@link NoRouteToHostException}, and
 * {@link SSLException}.
 */
@Internal
public class RemoteWriteRetryStrategy implements HttpRequestRetryStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteWriteRetryStrategy.class);

    /** List of exceptions considered non-recoverable (non-retryable). */
    private static final List<Class<? extends IOException>> NON_RECOVERABLE_EXCEPTIONS =
            Collections.unmodifiableList(
                    new ArrayList<Class<? extends IOException>>() {
                        {
                            add(InterruptedIOException.class);
                            add(UnknownHostException.class);
                            add(ConnectException.class);
                            add(NoRouteToHostException.class);
                            add(SSLException.class);
                        }
                    });

    private final long initialRetryDelayMs;
    private final long maxRetryDelayMs;
    private final int maxRetryCount;

    private final SinkMetricsCallback metricsCallback;

    public RemoteWriteRetryStrategy(
            PrometheusSinkConfiguration.RetryConfiguration retryConfiguration,
            SinkMetricsCallback metricsCallback) {
        this.initialRetryDelayMs = retryConfiguration.getInitialRetryDelayMS();
        this.maxRetryDelayMs = retryConfiguration.getMaxRetryDelayMS();
        this.maxRetryCount = retryConfiguration.getMaxRetryCount();
        this.metricsCallback = metricsCallback;
    }

    @Override
    public boolean retryRequest(
            HttpRequest httpRequest, IOException e, int execCount, HttpContext httpContext) {
        // Retry on any IOException except those considered non-recoverable
        boolean retry =
                (execCount <= maxRetryCount)
                        && !(NON_RECOVERABLE_EXCEPTIONS.contains(e.getClass()));
        LOG.debug(
                "{} retry on {}, at execution {}",
                (retry) ? "DO" : "DO NOT",
                e.getClass(),
                execCount);
        countRetry(retry);
        return retry;
    }

    @Override
    public boolean retryRequest(HttpResponse httpResponse, int execCount, HttpContext httpContext) {
        boolean retry = (execCount <= maxRetryCount) && (classify(httpResponse) == RETRYABLE_ERROR);
        LOG.debug(
                "{} retry on response {} {}, at execution {}",
                (retry) ? "DO" : "DO NOT",
                httpResponse.getCode(),
                httpResponse.getReasonPhrase(),
                execCount);
        countRetry(retry);
        return retry;
    }

    @Override
    public TimeValue getRetryInterval(
            HttpResponse httpResponse, int execCount, HttpContext httpContext) {
        long calculatedDelay = initialRetryDelayMs << (execCount - 1);
        return TimeValue.ofMilliseconds(Math.min(calculatedDelay, maxRetryDelayMs));
    }

    private void countRetry(boolean retry) {
        if (retry) {
            metricsCallback.onWriteRequestRetry();
        }
    }
}
