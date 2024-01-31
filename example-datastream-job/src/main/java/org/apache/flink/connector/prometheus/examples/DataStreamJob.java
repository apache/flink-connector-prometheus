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

package org.apache.flink.connector.prometheus.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeriesLabelsAndMetricNameKeySelector;
import org.apache.flink.connector.prometheus.sink.aws.AmazonManagedPrometheusWriteRequestSigner;
import org.apache.flink.connector.prometheus.sink.errorhandling.OnErrorBehavior;
import org.apache.flink.connector.prometheus.sink.errorhandling.SinkWriterErrorHandlingBehaviorConfiguration;
import org.apache.flink.connector.prometheus.sink.http.RetryConfiguration;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Supplier;

/** Test application testing the Prometheus sink connector. */
public class DataStreamJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env;

        // Conditionally return a local execution environment with
        if (args.length > 0 && Arrays.stream(args).anyMatch("--webUI"::equalsIgnoreCase)) {
            Configuration conf = new Configuration();
            conf.set(
                    ConfigOptions.key("rest.flamegraph.enabled").booleanType().noDefaultValue(),
                    true);
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        env.setParallelism(2);

        ParameterTool applicationParameters = ParameterTool.fromArgs(args);

        // Prometheus remote-write URL
        String prometheusRemoteWriteUrl = applicationParameters.get("prometheusRemoteWriteUrl");
        LOGGER.info("Prometheus URL:{}", prometheusRemoteWriteUrl);

        // Optionally configure Amazon Managed Prometheus request signer
        PrometheusRequestSigner requestSigner = null;
        String ampAWSRegion = applicationParameters.get("awsRegion");
        if (ampAWSRegion != null) {
            requestSigner =
                    new AmazonManagedPrometheusWriteRequestSigner(
                            prometheusRemoteWriteUrl, ampAWSRegion);
            LOGGER.info(
                    "Enable Amazon Managed Prometheus request-signer, region: {}", ampAWSRegion);
        }

        // Configure data generator
        int generatorMinSamplesPerTimeSeries = 1;
        int generatorMaxSamplesPerTimeSeries = 10;
        int generatorNumberOfDummyInstances = 5;
        long generatorPauseBetweenTimeSeriesMs = 100;
        LOGGER.info(
                "Data Generator configuration:"
                        + "\n\t\tMin samples per time series:{}\n\t\tMax samples per time series:{}\n\t\tPause between time series:{} ms"
                        + "\n\t\tNumber of dummy instances:{}",
                generatorMinSamplesPerTimeSeries,
                generatorMaxSamplesPerTimeSeries,
                generatorPauseBetweenTimeSeriesMs,
                generatorNumberOfDummyInstances);

        Supplier<PrometheusTimeSeries> eventGenerator =
                new SimpleInstanceMetricsTimeSeriesGenerator(
                                generatorMinSamplesPerTimeSeries,
                                generatorMaxSamplesPerTimeSeries,
                                generatorNumberOfDummyInstances)
                        .generator();

        SourceFunction<PrometheusTimeSeries> source =
                new FixedDelayDataGenertorSource<>(
                        PrometheusTimeSeries.class,
                        eventGenerator,
                        generatorPauseBetweenTimeSeriesMs);

        DataStream<PrometheusTimeSeries> prometheusTimeSeries = env.addSource(source);

        // Build the sink showing all supported configuration parameters.
        // It is not mandatory to specify all configurations, as they will fall back to the default
        // value
        AsyncSinkBase<PrometheusTimeSeries, Types.TimeSeries> sink =
                PrometheusSink.builder()
                        .setMaxBatchSizeInSamples(500)
                        .setMaxRecordSizeInSamples(500)
                        .setMaxTimeInBufferMS(5000)
                        .setRetryConfiguration(
                                RetryConfiguration.builder()
                                        .setInitialRetryDelayMS(30L)
                                        .setMaxRetryDelayMS(5000L)
                                        .setMaxRetryCount(100)
                                        .build())
                        .setSocketTimeoutMs(5000)
                        .setPrometheusRemoteWriteUrl(prometheusRemoteWriteUrl)
                        .setRequestSigner(requestSigner)
                        .setErrorHandlingBehaviourConfiguration(
                                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                                        .onPrometheusNonRetriableError(OnErrorBehavior.FAIL)
                                        .onMaxRetryExceeded(OnErrorBehavior.FAIL)
                                        .onHttpClientIOFail(OnErrorBehavior.FAIL)
                                        .build())
                        .build();

        prometheusTimeSeries
                .keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector())
                .sinkTo(sink);

        env.execute("Prometheus Sink test");
    }
}
