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

package org.apache.flink.connector.prometheus.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkBuilder;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

@Internal
public class PrometheusDynamicSink extends AsyncDynamicTableSink<Types.TimeSeries>
        implements SupportsPartitioning {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusDynamicSink.class);

    /** Consumed data type of the table. */
    private final DataType physicalDataType;

    private final PrometheusConfig prometheusConfig;

    protected PrometheusDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            @Nullable DataType physicalDataType,
            PrometheusConfig prometheusConfig) {

        super(
                maxBatchSize, // maxBatchSizeInSamples
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes, // maxRecordSizeInSamples
                maxTimeInBufferMS);

        this.physicalDataType =
                Preconditions.checkNotNull(physicalDataType, "Consumed data type must not be null");
        this.prometheusConfig = prometheusConfig;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        PrometheusSinkBuilder<RowData> builder =
                new PrometheusSinkBuilder<RowData>()
                        .setRetryConfiguration(
                                PrometheusSinkConfiguration.RetryConfiguration
                                        .DEFAULT_RETRY_CONFIGURATION)
                        .setElementConverter(
                                new RowDataElementConverter(physicalDataType, prometheusConfig))
                        .setPrometheusRemoteWriteUrl(prometheusConfig.getRemoteWriteEndpointUrl());

        Optional.ofNullable(prometheusConfig.getRequestSignerIdentifier())
                .ifPresent(
                        requestSignerId ->
                                builder.setRequestSigner(getRequestSigner(requestSignerId)));
        Optional.ofNullable(maxBatchSize).ifPresent(builder::setMaxBatchSizeInSamples);
        Optional.ofNullable(maxBufferSizeInBytes)
                .map(Long::intValue)
                .ifPresent(builder::setMaxRecordSizeInSamples);

        PrometheusSink<RowData> prometheusSink = builder.build();
        return SinkV2Provider.of(prometheusSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new PrometheusDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                physicalDataType,
                prometheusConfig);
    }

    @Override
    public String asSummaryString() {
        return "Prometheus";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partitions) {
        // We don't need to do anything here because the Prometheus sink handles a static partition
        // just like a normal partition.
    }

    private PrometheusRequestSigner getRequestSigner(String requestSignerIdentifier) {
        ServiceLoader<PrometheusDynamicRequestSignerFactory> loader =
                ServiceLoader.load(
                        PrometheusDynamicRequestSignerFactory.class, getClass().getClassLoader());

        Iterator<PrometheusDynamicRequestSignerFactory> factories = loader.iterator();
        while (true) {
            try {
                if (!factories.hasNext()) {
                    break;
                }

                PrometheusDynamicRequestSignerFactory factory = factories.next();
                if (factory.requestSignerIdentifer().equals(requestSignerIdentifier)) {
                    return factory.getRequestSigner(prometheusConfig);
                }
            } catch (ServiceConfigurationError serviceConfigurationError) {
                LOG.error(
                        "Error while attempting to iterate over request signer factories to "
                                + "locate request signer with identifier: '{}'",
                        requestSignerIdentifier,
                        serviceConfigurationError);
            }
        }

        LOG.error(
                "Unable to locate request signer factory for identifier: '{}'",
                requestSignerIdentifier);
        return null;
    }

    public static PrometheusDynamicSinkBuilder builder() {
        return new PrometheusDynamicSinkBuilder();
    }

    /** Builder class for {@link PrometheusDynamicSink}. */
    @Internal
    public static class PrometheusDynamicSinkBuilder
            extends AsyncDynamicTableSinkBuilder<Types.TimeSeries, PrometheusDynamicSinkBuilder> {
        private DataType physicalDataType;
        private PrometheusConfig prometheusConfig;

        public PrometheusDynamicSinkBuilder setPhysicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        public PrometheusDynamicSinkBuilder setPrometheusConfiguration(
                PrometheusConfig prometheusConfig) {
            this.prometheusConfig = prometheusConfig;
            return this;
        }

        @Override
        public AsyncDynamicTableSink<Types.TimeSeries> build() {
            return new PrometheusDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    physicalDataType,
                    prometheusConfig);
        }
    }
}
