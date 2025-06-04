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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_LABEL_KEYS;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_NAME;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REMOTE_WRITE_URL;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REQUEST_SIGNER;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_KEY;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_TIMESTAMP;
import static org.apache.flink.connector.prometheus.table.TestUtils.DATA_TYPE;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_LABEL_KEY;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_METRIC_NAME;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_REMOTE_WRITE_ENDPOINT;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_REQUEST_SIGNER;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_SAMPLE_KEY;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_SAMPLE_TS_KEY;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class PrometheusDynamicSinkFactoryTest {

    @Test
    void testGoodTableSink() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptions().build();

        // Construct actual sink
        PrometheusDynamicSink actualSink =
                (PrometheusDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected sink
        PrometheusDynamicSink expectedSink =
                (PrometheusDynamicSink)
                        PrometheusDynamicSink.builder()
                                .setPrometheusConfiguration(
                                        new PrometheusConfig(Configuration.fromMap(sinkOptions)))
                                .setPhysicalDataType(DATA_TYPE)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
        assertThat(actualSink.asSummaryString()).isEqualTo("Prometheus");
        assertThat(actualSink.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.upsert());

        Sink<RowData> createdSink =
                ((SinkV2Provider)
                                actualSink.getSinkRuntimeProvider(
                                        new SinkRuntimeProviderContext(false)))
                        .createSink();
        assertThat(createdSink).isInstanceOf(PrometheusSink.class);
    }

    @Test
    void testGoodTableSinkWithOptionalOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption(METRIC_REQUEST_SIGNER, TEST_REQUEST_SIGNER)
                        .build();

        // Construct actual sink
        PrometheusDynamicSink actualSink =
                (PrometheusDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected sink
        PrometheusDynamicSink expectedSink =
                (PrometheusDynamicSink)
                        PrometheusDynamicSink.builder()
                                .setPrometheusConfiguration(
                                        new PrometheusConfig(Configuration.fromMap(sinkOptions)))
                                .setPhysicalDataType(DATA_TYPE)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
    }

    @Test
    void testGoodTableSinkWithAsyncOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption(MAX_BATCH_SIZE, "100")
                        .withTableOption(MAX_IN_FLIGHT_REQUESTS, "200")
                        .withTableOption(MAX_BUFFERED_REQUESTS, "300")
                        .withTableOption(FLUSH_BUFFER_SIZE, "1024")
                        .withTableOption(FLUSH_BUFFER_TIMEOUT, "1000")
                        .build();

        // Construct actual sink
        PrometheusDynamicSink actualSink =
                (PrometheusDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected sink
        PrometheusDynamicSink expectedSink =
                (PrometheusDynamicSink)
                        PrometheusDynamicSink.builder()
                                .setPrometheusConfiguration(
                                        new PrometheusConfig(Configuration.fromMap(sinkOptions)))
                                .setMaxBatchSize(100)
                                .setMaxInFlightRequests(200)
                                .setMaxBufferedRequests(300)
                                .setMaxBufferSizeInBytes(1024)
                                .setMaxTimeInBufferMS(1000)
                                .setPhysicalDataType(DATA_TYPE)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
    }

    @Test
    void testBadTableSinkWithoutRequiredOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                new TableOptionsBuilder(
                                PrometheusDynamicSinkFactory.FACTORY_IDENTIFIER,
                                TestFormatFactory.IDENTIFIER)
                        .build();

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, Collections.emptyList(), sinkOptions))
                .havingCause()
                .withMessageContaining("One or more required options are missing.")
                .withMessageContaining(METRIC_NAME.key())
                .withMessageContaining(METRIC_LABEL_KEYS.key())
                .withMessageContaining(METRIC_SAMPLE_KEY.key())
                .withMessageContaining(METRIC_SAMPLE_TIMESTAMP.key())
                .withMessageContaining(METRIC_REMOTE_WRITE_URL.key());
    }

    @ParameterizedTest
    @MethodSource("provideBadMetricConfig")
    void testBadTableSinkWithBadMetricConfig(ConfigOption<String> configKey, String configValue) {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions().withTableOption(configKey, configValue).build();

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, Collections.emptyList(), sinkOptions))
                .havingCause()
                .withMessageContaining("is not a valid Prometheus metric config key")
                .withMessageContaining(configValue);
    }

    private static Stream<Arguments> provideBadMetricConfig() {
        return Stream.of(
                Arguments.of(METRIC_NAME, "bad_metric_name"),
                Arguments.of(METRIC_LABEL_KEYS, "bad_label_key"),
                Arguments.of(METRIC_SAMPLE_KEY, "bad_sample_key"),
                Arguments.of(METRIC_SAMPLE_TIMESTAMP, "bad_sample_ts_key"));
    }

    @Test
    void testCopyTableSink() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptions().build();

        // Construct actual sink
        PrometheusDynamicSink actualSink =
                (PrometheusDynamicSink) createTableSink(sinkSchema, sinkOptions);

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(actualSink.copy());
    }

    private ResolvedSchema defaultSinkSchema() {
        return ResolvedSchema.of(
                Column.physical(TEST_METRIC_NAME, DataTypes.STRING()),
                Column.physical(TEST_LABEL_KEY, DataTypes.STRING()),
                Column.physical(TEST_SAMPLE_KEY, DataTypes.DOUBLE()),
                Column.physical(TEST_SAMPLE_TS_KEY, DataTypes.TIMESTAMP(6)));
    }

    private TableOptionsBuilder defaultTableOptions() {
        String connector = PrometheusDynamicSinkFactory.FACTORY_IDENTIFIER;
        String format = TestFormatFactory.IDENTIFIER;
        return new TableOptionsBuilder(connector, format)
                // default table options
                .withTableOption(METRIC_NAME, TEST_METRIC_NAME)
                .withTableOption(METRIC_LABEL_KEYS, TEST_LABEL_KEY)
                .withTableOption(METRIC_SAMPLE_KEY, TEST_SAMPLE_KEY)
                .withTableOption(METRIC_SAMPLE_TIMESTAMP, TEST_SAMPLE_TS_KEY)
                .withTableOption(METRIC_REMOTE_WRITE_URL, TEST_REMOTE_WRITE_ENDPOINT);
    }
}
