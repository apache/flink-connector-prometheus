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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_LABEL_KEYS;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_NAME;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REMOTE_WRITE_URL;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REQUEST_SIGNER;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_KEY;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_TIMESTAMP;

/** Factory for creating {@link PrometheusDynamicSink}. */
@Internal
public class PrometheusDynamicSinkFactory extends AsyncDynamicTableSinkFactory {
    public static final String FACTORY_IDENTIFIER = "prometheus";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper factoryHelper =
                FactoryUtil.createTableFactoryHelper(this, context);
        ResolvedCatalogTable catalogTable = context.getCatalogTable();

        FactoryUtil.validateFactoryOptions(this, factoryHelper.getOptions());

        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        PrometheusConfig prometheusConfig = new PrometheusConfig(factoryHelper.getOptions());
        validateMetricConfigKeys(prometheusConfig, physicalDataType);

        PrometheusDynamicSink.PrometheusDynamicSinkBuilder builder =
                PrometheusDynamicSink.builder()
                        .setPhysicalDataType(physicalDataType)
                        .setPrometheusConfiguration(prometheusConfig);

        addAsyncOptionsToBuilder(getAsyncSinkOptions(factoryHelper.getOptions()), builder);

        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();

        requiredOptions.add(METRIC_NAME);
        requiredOptions.add(METRIC_LABEL_KEYS);
        requiredOptions.add(METRIC_SAMPLE_KEY);
        requiredOptions.add(METRIC_SAMPLE_TIMESTAMP);
        requiredOptions.add(METRIC_REMOTE_WRITE_URL);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        optionalOptions.add(METRIC_REQUEST_SIGNER);

        return optionalOptions;
    }

    private Properties getAsyncSinkOptions(ReadableConfig config) {
        Properties properties = new Properties();
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE))
                .ifPresent(
                        flushBufferSize ->
                                properties.put(
                                        AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE.key(),
                                        flushBufferSize));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.MAX_BATCH_SIZE))
                .ifPresent(
                        maxBatchSize ->
                                properties.put(
                                        AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key(),
                                        maxBatchSize));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS))
                .ifPresent(
                        maxInflightRequests ->
                                properties.put(
                                        AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key(),
                                        maxInflightRequests));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS))
                .ifPresent(
                        maxBufferedRequests ->
                                properties.put(
                                        AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS.key(),
                                        maxBufferedRequests));
        Optional.ofNullable(config.get(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT))
                .ifPresent(
                        timeout ->
                                properties.put(
                                        AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key(),
                                        timeout));
        return properties;
    }

    private void validateMetricConfigKeys(
            PrometheusConfig prometheusConfig, DataType physicalDataType) {
        Set<String> fieldNames =
                DataType.getFields(physicalDataType).stream()
                        .map(DataTypes.AbstractField::getName)
                        .collect(Collectors.toSet());

        List<String> metricConfigKeys =
                Stream.concat(
                                Stream.of(
                                        prometheusConfig.getMetricName(),
                                        prometheusConfig.getMetricSampleKey(),
                                        prometheusConfig.getMetricSampleTimestamp()),
                                prometheusConfig.getLabelKeys().stream())
                        .collect(Collectors.toList());

        metricConfigKeys.forEach(
                metricConfigKey ->
                        Optional.ofNullable(metricConfigKey)
                                .filter(fieldNames::contains)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        String.format(
                                                                "%s is not a valid Prometheus metric config key, valid keys are: %s",
                                                                metricConfigKey, fieldNames))));
    }
}
