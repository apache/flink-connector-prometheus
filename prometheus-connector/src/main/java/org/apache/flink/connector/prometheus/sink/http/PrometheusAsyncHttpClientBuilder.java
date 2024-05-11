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

import org.apache.flink.connector.prometheus.sink.metrics.SinkMetricsCallback;
import org.apache.flink.util.Preconditions;

import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;

import java.io.Serializable;
import java.util.Optional;

/**
 * Builder for async http client that will retry, based on the {@link RemoteWriteRetryStrategy}
 * specified.
 */
public class PrometheusAsyncHttpClientBuilder implements Serializable {
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 5000;

    private final RetryConfiguration retryConfiguration;
    private Integer socketTimeoutMs;

    public PrometheusAsyncHttpClientBuilder(RetryConfiguration retryConfiguration) {
        this.retryConfiguration = retryConfiguration;
    }

    public PrometheusAsyncHttpClientBuilder setSocketTimeout(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
        return this;
    }

    public CloseableHttpAsyncClient buildAndStartClient(SinkMetricsCallback metricsCallback) {
        int actualSocketTimeoutMs =
                Optional.ofNullable(socketTimeoutMs).orElse(DEFAULT_SOCKET_TIMEOUT_MS);

        Preconditions.checkArgument(
                retryConfiguration.getInitialRetryDelayMS() >= 0,
                "Initial retry delay must be >= 0");
        Preconditions.checkArgument(
                retryConfiguration.getMaxRetryDelayMS()
                        >= retryConfiguration.getInitialRetryDelayMS(),
                "Max retry delay must be >= initial retry delay");
        Preconditions.checkArgument(
                retryConfiguration.getMaxRetryCount() >= 0, "Max retry count must be >= 0");
        Preconditions.checkArgument(actualSocketTimeoutMs >= 0, "Socket timeout must be >= 0");

        final IOReactorConfig ioReactorConfig =
                IOReactorConfig.custom()
                        // Remote-Writes must be single-threaded to prevent out-of-order writes
                        .setIoThreadCount(1)
                        .setSoTimeout(Timeout.ofMilliseconds(actualSocketTimeoutMs))
                        .build();

        TlsConfig tlsConfig =
                TlsConfig.custom().setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1).build();
        PoolingAsyncClientConnectionManager connectionManager =
                PoolingAsyncClientConnectionManagerBuilder.create()
                        .setDefaultTlsConfig(tlsConfig)
                        .build();
        CloseableHttpAsyncClient client =
                HttpAsyncClients.custom()
                        .setConnectionManager(connectionManager)
                        .setIOReactorConfig(ioReactorConfig)
                        .setRetryStrategy(
                                new RemoteWriteRetryStrategy(retryConfiguration, metricsCallback))
                        .build();

        client.start();
        return client;
    }
}
