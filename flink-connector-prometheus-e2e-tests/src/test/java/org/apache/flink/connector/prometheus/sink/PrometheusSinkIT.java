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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the Prometheus Sink Connector against a containerized Prometheus instance.
 *
 * <p>These tests validate end-to-end behavior: writing {@link PrometheusTimeSeries} data through
 * the {@link PrometheusSink} and verifying it arrives in Prometheus via the query API.
 */
@Testcontainers
public class PrometheusSinkIT {

    private static final int PARALLELISM = 1;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @Container private static final PrometheusContainer PROMETHEUS = new PrometheusContainer();

    private static PrometheusQueryClient queryClient;

    @BeforeAll
    static void setUp() {
        queryClient = new PrometheusQueryClient(PROMETHEUS.getBaseUrl());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (queryClient != null) {
            queryClient.close();
        }
    }

    @Test
    void singleTimeSeriesWrittenAndQueryable() throws Exception {
        String metricName = "integration_test_single_metric";
        double expectedValue = 42.0;
        long timestamp = System.currentTimeMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        PrometheusTimeSeries timeSeries =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("instance", "test-instance-1")
                        .addLabel("job", "integration-test")
                        .addSample(expectedValue, timestamp)
                        .build();

        DataStream<PrometheusTimeSeries> stream =
                env.fromCollection(
                        new ArrayList<>(Collections.singletonList(timeSeries)),
                        TypeInformation.of(PrometheusTimeSeries.class));

        stream.sinkTo(buildSink());

        env.execute("PrometheusSinkIT - single time series");

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            int count = queryClient.queryMetricCount(metricName);
                            assertThat(count).isGreaterThanOrEqualTo(1);
                        });

        List<Double> values = queryClient.queryMetricValues(metricName);
        assertThat(values).contains(expectedValue);

        assertThat(queryClient.queryMetricHasLabel(metricName, "instance", "test-instance-1"))
                .isTrue();
        assertThat(queryClient.queryMetricHasLabel(metricName, "job", "integration-test")).isTrue();
    }

    @Test
    void multipleTimeSeriesWithDifferentLabels() throws Exception {
        String metricName = "integration_test_multi_labels";
        long timestamp = System.currentTimeMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        PrometheusTimeSeries ts1 =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("instance", "host-a")
                        .addLabel("region", "eu-west-1")
                        .addSample(10.0, timestamp)
                        .build();

        PrometheusTimeSeries ts2 =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("instance", "host-b")
                        .addLabel("region", "us-east-1")
                        .addSample(20.0, timestamp)
                        .build();

        DataStream<PrometheusTimeSeries> stream =
                env.fromCollection(
                        new ArrayList<>(Arrays.asList(ts1, ts2)),
                        TypeInformation.of(PrometheusTimeSeries.class));

        stream.sinkTo(buildSink());

        env.execute("PrometheusSinkIT - multiple time series");

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            int count = queryClient.queryMetricCount(metricName);
                            assertThat(count).isEqualTo(2);
                        });

        assertThat(queryClient.queryMetricHasLabel(metricName, "instance", "host-a")).isTrue();
        assertThat(queryClient.queryMetricHasLabel(metricName, "instance", "host-b")).isTrue();
    }

    @Test
    void timeSeriesWithMultipleSamples() throws Exception {
        String metricName = "integration_test_multi_samples";
        long baseTimestamp = System.currentTimeMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        PrometheusTimeSeries timeSeries =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("instance", "sampler")
                        .addSample(1.0, baseTimestamp)
                        .addSample(2.0, baseTimestamp + 1000)
                        .addSample(3.0, baseTimestamp + 2000)
                        .build();

        DataStream<PrometheusTimeSeries> stream =
                env.fromCollection(
                        new ArrayList<>(Collections.singletonList(timeSeries)),
                        TypeInformation.of(PrometheusTimeSeries.class));

        stream.sinkTo(buildSink());

        env.execute("PrometheusSinkIT - multiple samples");

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            int count = queryClient.queryMetricCount(metricName);
                            assertThat(count).isGreaterThanOrEqualTo(1);
                        });

        List<Double> values = queryClient.queryMetricValues(metricName);
        assertThat(values).contains(3.0);
    }

    @Test
    void highThroughputWriteFromSequenceSource() throws Exception {
        final String metricName = "integration_test_throughput";
        final int numElements = 100;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        DataStream<PrometheusTimeSeries> stream =
                env.fromSource(
                                new NumberSequenceSource(0, numElements - 1),
                                WatermarkStrategy.noWatermarks(),
                                "sequence-source")
                        .map(new ThroughputTestMapper(metricName));

        stream.sinkTo(buildSink());

        env.execute("PrometheusSinkIT - high throughput");

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(3))
                .untilAsserted(
                        () -> {
                            int count = queryClient.queryMetricCount(metricName);
                            assertThat(count).isEqualTo(numElements);
                        });
    }

    private static class ThroughputTestMapper implements MapFunction<Long, PrometheusTimeSeries> {

        private final String metricName;

        ThroughputTestMapper(String metricName) {
            this.metricName = metricName;
        }

        @Override
        public PrometheusTimeSeries map(Long i) {
            long ts = System.currentTimeMillis();
            return PrometheusTimeSeries.builder()
                    .withMetricName(metricName)
                    .addLabel("index", String.valueOf(i))
                    .addSample(i.doubleValue(), ts)
                    .build();
        }
    }

    @Test
    void sinkWithKeyByForParallelismGreaterThanOne() throws Exception {
        String metricName = "integration_test_keyed";
        long timestamp = System.currentTimeMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        PrometheusTimeSeries ts1 =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("partition", "A")
                        .addSample(100.0, timestamp)
                        .build();

        PrometheusTimeSeries ts2 =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("partition", "B")
                        .addSample(200.0, timestamp)
                        .build();

        PrometheusTimeSeries ts3 =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("partition", "A")
                        .addSample(150.0, timestamp + 1000)
                        .build();

        DataStream<PrometheusTimeSeries> stream =
                env.fromCollection(
                        new ArrayList<>(Arrays.asList(ts1, ts2, ts3)),
                        TypeInformation.of(PrometheusTimeSeries.class));

        stream.keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector()).sinkTo(buildSink());

        env.execute("PrometheusSinkIT - keyed parallelism");

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            int count = queryClient.queryMetricCount(metricName);
                            assertThat(count).isEqualTo(2);
                        });

        assertThat(queryClient.queryMetricHasLabel(metricName, "partition", "A")).isTrue();
        assertThat(queryClient.queryMetricHasLabel(metricName, "partition", "B")).isTrue();
    }

    private PrometheusSink buildSink() {
        return (PrometheusSink)
                PrometheusSink.builder()
                        .setPrometheusRemoteWriteUrl(PROMETHEUS.getRemoteWriteUrl())
                        .setMaxBatchSizeInSamples(500)
                        .setMaxRecordSizeInSamples(500)
                        .setMaxTimeInBufferMS(1000)
                        .setRetryConfiguration(
                                PrometheusSinkConfiguration.RetryConfiguration.builder()
                                        .setInitialRetryDelayMS(100L)
                                        .setMaxRetryDelayMS(1000L)
                                        .setMaxRetryCount(5)
                                        .build())
                        .setSocketTimeoutMs(5000)
                        .setErrorHandlingBehaviorConfiguration(
                                PrometheusSinkConfiguration
                                        .SinkWriterErrorHandlingBehaviorConfiguration.builder()
                                        .onMaxRetryExceeded(
                                                PrometheusSinkConfiguration.OnErrorBehavior.FAIL)
                                        .onPrometheusNonRetryableError(
                                                PrometheusSinkConfiguration.OnErrorBehavior
                                                        .DISCARD_AND_CONTINUE)
                                        .build())
                        .build();
    }
}
