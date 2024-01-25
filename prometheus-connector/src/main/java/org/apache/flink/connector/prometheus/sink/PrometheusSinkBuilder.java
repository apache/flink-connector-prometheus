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

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.prometheus.sink.http.PrometheusAsyncHttpClientBuilder;
import org.apache.flink.connector.prometheus.sink.http.RetryConfiguration;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

/** Builder for Sink implementation. */
public class PrometheusSinkBuilder
        extends AsyncSinkBaseBuilder<
                PrometheusTimeSeries, Types.TimeSeries, PrometheusSinkBuilder> {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkBuilder.class);

    private static final int DEFAULT_MAX_BATCH_SIZE_IN_SAMPLES = 500;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS =
            1000; // Max nr of requestEntry that will be buffered

    private String prometheusRemoteWriteUrl;
    private RetryConfiguration retryConfiguration;
    private Integer socketTimeoutMs;
    private PrometheusRequestSigner requestSigner = null;
    private Integer maxBatchSizeInSamples;
    private Integer maxRecordSizeInSamples;
    private String httpUserAgent = null;

    @Override
    public AsyncSinkBase<PrometheusTimeSeries, Types.TimeSeries> build() {

        int maxInFlightRequest = 1;

        int actualMaxBatchSizeInSamples =
                Optional.ofNullable(getMaxBatchSizeInSamples())
                        .orElse(DEFAULT_MAX_BATCH_SIZE_IN_SAMPLES);
        int actualMaxBufferedRequests =
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS);
        long actualMaxTimeInBufferMS =
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS);
        int actualMaxRecordSizeInSamples =
                Optional.ofNullable(getMaxRecordSizeInSamples())
                        .orElse(getMaxBatchSizeInSamples()); // By default, set max record size to
        // max batch size (in samples)

        int actualSocketTimeoutMs =
                Optional.ofNullable(getSocketTimeoutMs())
                        .orElse(PrometheusAsyncHttpClientBuilder.DEFAULT_SOCKET_TIMEOUT_MS);

        String actualHttpUserAgent =
                Optional.ofNullable(getHttpUserAgent())
                        .orElse(PrometheusRemoteWriteHttpRequestBuilder.DEFAULT_USER_AGENT);

        Preconditions.checkArgument(
                StringUtils.isNotBlank(prometheusRemoteWriteUrl),
                "Missing or blank Prometheus Remote-Write URL");
        checkValidRemoteWriteUrl(prometheusRemoteWriteUrl);
        Preconditions.checkNotNull(retryConfiguration, "Missing retry configuration");
        Preconditions.checkArgument(
                actualMaxBatchSizeInSamples > 0, "Max batch size (in samples) must be positive");
        Preconditions.checkArgument(
                actualMaxRecordSizeInSamples <= actualMaxBatchSizeInSamples,
                "Max record size (in samples) must be <= Max batch size");

        LOG.info(
                "Prometheus sink configuration:"
                        + "\n\t\tmaxBatchSizeInSamples={}\n\t\tmaxRecordSizeInSamples={}"
                        + "\n\t\tmaxTimeInBufferMs={}\n\t\tmaxInFlightRequests={}\n\t\tmaxBufferedRequests={}"
                        + "\n\t\tinitialRetryDelayMs={}\n\t\tmaxRetryDelayMs={}\n\t\tmaxRetryCount={}"
                        + "\n\t\tsocketTimeoutMs={}\n\t\thttpUserAgent={}",
                actualMaxBatchSizeInSamples,
                actualMaxRecordSizeInSamples,
                actualMaxTimeInBufferMS,
                maxInFlightRequest,
                actualMaxBufferedRequests,
                retryConfiguration.getInitialRetryDelayMS(),
                retryConfiguration.getMaxRetryDelayMS(),
                retryConfiguration.getMaxRetryCount(),
                socketTimeoutMs,
                actualHttpUserAgent);

        return new PrometheusSink(
                new PrometheusTimeSeriesConverter(),
                maxInFlightRequest,
                actualMaxBufferedRequests,
                actualMaxBatchSizeInSamples,
                actualMaxRecordSizeInSamples,
                actualMaxTimeInBufferMS,
                prometheusRemoteWriteUrl,
                new PrometheusAsyncHttpClientBuilder(retryConfiguration)
                        .setSocketTimeout(actualSocketTimeoutMs),
                requestSigner,
                actualHttpUserAgent);
    }

    private static void checkValidRemoteWriteUrl(String url) {
        try {
            new URL(url);
        } catch (MalformedURLException mue) {
            throw new IllegalArgumentException("Invalid Remote-Write URL: " + url, mue);
        }
    }

    public PrometheusSinkBuilder setPrometheusRemoteWriteUrl(String prometheusRemoteWriteUrl) {
        this.prometheusRemoteWriteUrl = prometheusRemoteWriteUrl;
        return this;
    }

    public PrometheusSinkBuilder setRequestSigner(PrometheusRequestSigner requestSigner) {
        this.requestSigner = requestSigner;
        return this;
    }

    public PrometheusSinkBuilder setMaxBatchSizeInSamples(int maxBatchSizeInSamples) {
        this.maxBatchSizeInSamples = maxBatchSizeInSamples;
        return this;
    }

    public PrometheusSinkBuilder setMaxRecordSizeInSamples(int maxRecordSizeInSamples) {
        this.maxRecordSizeInSamples = maxRecordSizeInSamples;
        return this;
    }

    public PrometheusSinkBuilder setRetryConfiguration(RetryConfiguration retryConfiguration) {
        this.retryConfiguration = retryConfiguration;
        return this;
    }

    public PrometheusSinkBuilder setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
        return this;
    }

    public PrometheusSinkBuilder setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
        return this;
    }

    private Integer getMaxBatchSizeInSamples() {
        return maxBatchSizeInSamples;
    }

    private Integer getMaxRecordSizeInSamples() {
        return maxRecordSizeInSamples;
    }

    public RetryConfiguration getRetryConfiguration() {
        return retryConfiguration;
    }

    public Integer getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    /// Disable setting maxBatchSize, maxBatchSizeInBytes, and maxRecordSizeInBytes directly

    @Override
    public PrometheusSinkBuilder setMaxBatchSize(int maxBatchSize) {
        throw new UnsupportedOperationException("maxBatchSize is not supported by this sink");
    }

    @Override
    public PrometheusSinkBuilder setMaxBatchSizeInBytes(long maxBatchSizeInBytes) {
        throw new UnsupportedOperationException(
                "maxBatchSizeInBytes is not supported by this sink");
    }

    @Override
    public PrometheusSinkBuilder setMaxRecordSizeInBytes(long maxRecordSizeInBytes) {
        throw new UnsupportedOperationException(
                "maxRecordSizeInBytes is not supported by this sink");
    }

    @Override
    protected Integer getMaxBatchSize() {
        throw new UnsupportedOperationException("maxBatchSize is not supported by this sink");
    }

    @Override
    protected Long getMaxBatchSizeInBytes() {
        throw new UnsupportedOperationException(
                "maxRecordSizeInBytes is not supported by this sink");
    }

    @Override
    protected Long getMaxRecordSizeInBytes() {
        throw new UnsupportedOperationException(
                "maxRecordSizeInBytes is not supported by this sink");
    }
}
