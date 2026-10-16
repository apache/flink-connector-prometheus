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

package org.apache.flink.connector.prometheus.sink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.OnErrorBehavior;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeriesLabelsAndMetricNameKeySelector;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Example application demonstrating the usage of the Prometheus sink connector.
 *
 * <p>The application expects a single configuration parameter, with the RemoteWrite endpoint URL:
 * --prometheusRemoteWriteUrl &lt;URL&gt;
 *
 * <p>The application generates random TimeSeries internally and sinks to Prometheus.
 *
 * <p>It also demonstrates how to keyBy the stream before the sink, to support a sink parallelism
 * greater than 1.
 */
public class DataStreamExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Setting parallelism > 1 to demonstrate usage of
        // PrometheusTimeSeriesLabelsAndMetricNameKeySelector
        // to parallelize writers without causing any out-of-order write rejection from Prometheus
        env.setParallelism(2);

        ParameterTool applicationParameters = ParameterTool.fromArgs(args);

        // Prometheus remote-write URL
        String prometheusRemoteWriteUrl = applicationParameters.get("prometheusRemoteWriteUrl");
        LOGGER.info("Prometheus URL:{}", prometheusRemoteWriteUrl);

        // Optionally configure a request signer
        PrometheusRequestSigner requestSigner = null;

        // Uncomment the following line to enable the AmazonManagedPrometheusWriteRequestSigner
        // for signing requests when writing to Amazon Managed Prometheus
        //        requestSigner =
        //                new AmazonManagedPrometheusWriteRequestSigner(
        //                        prometheusRemoteWriteUrl, applicationParameters.get("awsRegion"));

        // Configure data generator
        int generatorMinSamplesPerTimeSeries = 1;
        int generatorMaxSamplesPerTimeSeries = 10;
        int generatorNumberOfSources = 10;
        short generatorNumberOfMetricsPerSource = 5;
        LOGGER.info(
                "Data Generator configuration:"
                        + "\n\t\tMin samples per time series:{}\n\t\tMax samples per time series:{}"
                        + "\n\t\tNumber of sources:{}\n\t\tNumber of metrics per source:{}",
                generatorMinSamplesPerTimeSeries,
                generatorMaxSamplesPerTimeSeries,
                generatorNumberOfSources,
                generatorNumberOfMetricsPerSource);

        Supplier<PrometheusTimeSeries> eventGenerator =
                new SimpleRandomMetricTimeSeriesGenerator(
                                generatorMinSamplesPerTimeSeries,
                                generatorMaxSamplesPerTimeSeries,
                                generatorNumberOfSources,
                                generatorNumberOfMetricsPerSource)
                        .generator();

        // Use NumberSequenceSource (FLIP-27 Source API) to generate a sequence of longs,
        // then map each element to a PrometheusTimeSeries using the event generator.
        DataStream<PrometheusTimeSeries> prometheusTimeSeries =
                env.fromSource(
                                new NumberSequenceSource(0, Long.MAX_VALUE),
                                WatermarkStrategy.noWatermarks(),
                                "data-generator")
                        .map(i -> eventGenerator.get())
                        .returns(PrometheusTimeSeries.class);

        // Build the sink showing all supported configuration parameters.
        // It is not mandatory to specify all configurations, as they will fall back to the default
        // value
        AsyncSinkBase<PrometheusTimeSeries, Types.TimeSeries> sink =
                PrometheusSink.builder()
                        .setPrometheusRemoteWriteUrl(prometheusRemoteWriteUrl)
                        // If the Prometheus implementation expects
                        // authentication, a valid signer implementation
                        // must be provided
                        .setRequestSigner(requestSigner) // Optional
                        .setMaxBatchSizeInSamples(500) // Optional, default 500
                        .setMaxRecordSizeInSamples(500) // Optional, default = maxBatchSizeInSamples
                        .setMaxTimeInBufferMS(5000) // Optional, default 5000 ms
                        .setRetryConfiguration(
                                PrometheusSinkConfiguration.RetryConfiguration.builder()
                                        .setInitialRetryDelayMS(30L) // Optional, default 30 ms
                                        .setMaxRetryDelayMS(5000L) // Optional, default 5000 ms
                                        .setMaxRetryCount(100) // Optional, default 100
                                        .build())
                        .setSocketTimeoutMs(5000) // Optional, default 5000 ms
                        // If no Error Handling Behavior configuration is provided, all behaviors
                        // default to FAIL
                        .setErrorHandlingBehaviorConfiguration(
                                PrometheusSinkConfiguration
                                        .SinkWriterErrorHandlingBehaviorConfiguration.builder()
                                        .onMaxRetryExceeded(
                                                OnErrorBehavior.FAIL) // Optional, default FAIL
                                        .build())
                        .setMetricGroupName("Prometheus") // Optional, default "Prometheus"
                        .build();

        // Key the stream using the provided KeySelector, before sinking to Prometheus.
        // This is required when running with parallelism > 1, to guarantee that all the TimeSeries
        // with the same set of
        // Labels are written by the same subtask, to prevent accidental out-of-order writes.
        prometheusTimeSeries
                .keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector())
                .sinkTo(sink);

        env.execute("Prometheus Sink test");
    }

    /**
     * Simple dummy TimeSeries generator, generating random samples for a given number of "metrics"
     * and a given number of "sources".
     *
     * <p>Sample timestamp is always the current system time. The value is random, between 0 and 1.
     */
    public static class SimpleRandomMetricTimeSeriesGenerator implements Serializable {

        private final int numberOfSources;
        private final int minNrOfSamples;
        private final int maxNrOfSamples;
        private final short numberOfMetricsPerSource;

        public SimpleRandomMetricTimeSeriesGenerator(
                int minNrOfSamples,
                int maxNrOfSamples,
                int numberOfSources,
                short numberOfMetricsPerSource) {
            this.numberOfSources = numberOfSources;
            this.minNrOfSamples = minNrOfSamples;
            this.maxNrOfSamples = maxNrOfSamples;
            this.numberOfMetricsPerSource = numberOfMetricsPerSource;
        }

        private String randomMetricName() {
            short metricNr = (short) (RandomUtils.nextDouble(0, 1) * numberOfMetricsPerSource);
            return String.format("M%05d", metricNr);
        }

        private String randomSourceId() {
            int sourceId = RandomUtils.nextInt(0, numberOfSources);
            return String.format("S%010d", sourceId);
        }

        private PrometheusTimeSeries nextTimeSeries(int minNrOfSamples, int maxNrOfSamples) {
            String instanceId = randomSourceId();

            int nrOfSamples = RandomUtils.nextInt(minNrOfSamples, maxNrOfSamples + 1);
            String metricName = randomMetricName();

            PrometheusTimeSeries.Builder builder =
                    PrometheusTimeSeries.builder()
                            .withMetricName(metricName)
                            .addLabel("SourceID", instanceId);
            for (int i = 0; i < nrOfSamples; i++) {
                double value = RandomUtils.nextDouble(0, 1);
                builder.addSample(value, System.currentTimeMillis());
            }
            return builder.build();
        }

        public Supplier<PrometheusTimeSeries> generator() {
            return (Supplier<PrometheusTimeSeries> & Serializable)
                    () -> nextTimeSeries(minNrOfSamples, maxNrOfSamples);
        }
    }
}
