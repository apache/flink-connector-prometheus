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

import org.apache.flink.connector.prometheus.sink.SinkCounters;
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
import java.util.List;

public class RemoteWriteRetryStrategy implements HttpRequestRetryStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteWriteRetryStrategy.class);

    private static final List<Class<? extends IOException>> NON_RETRIABLE_EXCEPTIONS = List.of(
            InterruptedIOException.class,
            UnknownHostException.class,
            ConnectException.class,
            NoRouteToHostException.class,
            SSLException.class);

    private final long initialRetryDelayMs;
    private final long maxRetryDelayMs;
    private final int maxRetryCount;

    private final SinkCounters counters;

    public RemoteWriteRetryStrategy(RetryConfiguration retryConfiguration, SinkCounters counters) {
        this.initialRetryDelayMs = retryConfiguration.getInitialRetryDelayMS();
        this.maxRetryDelayMs = retryConfiguration.getMaxRetryDelayMS();
        this.maxRetryCount = retryConfiguration.getMaxRetryCount();
        this.counters = counters;
    }

    @Override
    public boolean retryRequest(HttpRequest httpRequest, IOException e, int execCount, HttpContext httpContext) {
        // Retry on any IOException except those considered non-retriable
        var retry = (execCount <= maxRetryCount) && !(NON_RETRIABLE_EXCEPTIONS.contains(e.getClass()));
        LOG.debug("{} retry on {}, at execution {}", (retry) ? "DO" : "DO NOT", e.getClass(), execCount);
        countRetry(retry);
        return retry;
    }

    @Override
    public boolean retryRequest(HttpResponse httpResponse, int execCount, HttpContext httpContext) {
        var retry = (execCount <= maxRetryCount) && RemoteWriteResponseClassifier.isRetriableErrorResponse(httpResponse);
        LOG.debug("{} retry on response {} {}, at execution {}", (retry) ? "DO" : "DO NOT", httpResponse.getCode(), httpResponse.getReasonPhrase(), execCount);
        countRetry(retry);
        return retry;
    }

    @Override
    public TimeValue getRetryInterval(HttpResponse httpResponse, int execCount, HttpContext httpContext) {
        long calculatedDelay = initialRetryDelayMs << (execCount - 1);
        return TimeValue.ofMilliseconds(Math.min(calculatedDelay, maxRetryDelayMs));
    }

    private void countRetry(boolean retry) {
        if (retry) {
            counters.inc(SinkCounters.SinkCounter.NUM_WRITE_REQUESTS_RETRIES);
        }
    }
}
