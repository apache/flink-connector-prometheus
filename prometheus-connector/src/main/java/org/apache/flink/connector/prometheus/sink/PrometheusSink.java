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
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.prometheus.sink.http.PrometheusAsyncHttpClientBuilder;
import org.apache.flink.connector.prometheus.sink.http.RetryConfiguration;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;

import java.util.Collection;

public class PrometheusSink extends AsyncSinkBase<PrometheusTimeSeries, Types.TimeSeries> {
    private final String prometheusRemoteWriteUrl;
    private final PrometheusAsyncHttpClientBuilder clientBuilder;
    private final PrometheusRequestSigner requestSigner;

    private final int maxBatchSizeInSamples;

    protected PrometheusSink(
            ElementConverter<PrometheusTimeSeries, Types.TimeSeries> elementConverter,
            int maxInFlightRequests,
            int maxBufferedRequests,
            int maxBatchSizeInSamples,
            int maxRecordSizeInSamples,
            long maxTimeInBufferMS,
            String prometheusRemoteWriteUrl,
            PrometheusAsyncHttpClientBuilder clientBuilder,
            PrometheusRequestSigner requestSigner) {
        super(
                elementConverter,
                maxBatchSizeInSamples, // maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInSamples, // maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInSamples // maxRecordSizeInBytes
        );
        this.maxBatchSizeInSamples = maxBatchSizeInSamples;
        this.requestSigner = requestSigner;
        this.prometheusRemoteWriteUrl = prometheusRemoteWriteUrl;
        this.clientBuilder = clientBuilder;
    }

    public int getMaxBatchSizeInSamples() {
        return maxBatchSizeInSamples;
    }

    @Override
    public StatefulSinkWriter<PrometheusTimeSeries, BufferedRequestState<Types.TimeSeries>> createWriter(InitContext initContext) {
        SinkCounters counters = SinkCounters.buildSinkCounters(initContext.metricGroup());
        CloseableHttpAsyncClient asyncHttpClient = clientBuilder.buildAndStartClient(counters);

        return new PrometheusSinkWriter(
                getElementConverter(),
                initContext,
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInSamples(),
                getMaxTimeInBufferMS(),
                prometheusRemoteWriteUrl,
                asyncHttpClient,
                counters,
                requestSigner);
    }

    @Override
    public StatefulSinkWriter<PrometheusTimeSeries, BufferedRequestState<Types.TimeSeries>> restoreWriter(InitContext initContext, Collection<BufferedRequestState<Types.TimeSeries>> recoveredState) {
        SinkCounters counters = SinkCounters.buildSinkCounters(initContext.metricGroup());
        CloseableHttpAsyncClient asyncHttpClient = clientBuilder.buildAndStartClient(counters);
        return new PrometheusSinkWriter(
                getElementConverter(),
                initContext,
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInSamples(),
                getMaxTimeInBufferMS(),
                prometheusRemoteWriteUrl,
                asyncHttpClient,
                counters,
                requestSigner,
                recoveredState);
    }

    public static PrometheusSinkBuilder builder() {
        return new PrometheusSinkBuilder();
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<Types.TimeSeries>> getWriterStateSerializer() {
        return new PrometheusStateSerializer();
    }
}
