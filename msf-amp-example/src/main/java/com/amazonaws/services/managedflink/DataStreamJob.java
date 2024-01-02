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

package com.amazonaws.services.managedflink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeriesLabelsAndMetricNameKeySelector;
import org.apache.flink.connector.prometheus.sink.aws.AmazonManagedPrometheusWriteRequestSigner;
import org.apache.flink.connector.prometheus.sink.http.RetryConfiguration;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.managedflink.domain.SimpleInstanceMetricsTimeSeriesGenerator;
import com.amazonaws.services.managedflink.source.ParallelFixedPaceSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;

/**
 * Test application, designed to run on Amazon Managed Service for Apache Flink and write to Amazon
 * Managed Prometheus.
 *
 * <p>To run locally the application expects this cli args: --Region=&lg;region&gt;
 * --PrometheusWriteUrl=&lg;prometheus-write-url&gt; (optional) --webUI
 */
public class DataStreamJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamJob.class);

    private static final String APPLICATION_CONFIG_GROUP = "FlinkApplicationProperties";

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    private static ParameterTool loadApplicationParameters(
            String[] args, StreamExecutionEnvironment env) throws IOException {
        if (isLocal(env)) {
            return ParameterTool.fromArgs(args);
        } else {
            Map<String, Properties> applicationProperties =
                    KinesisAnalyticsRuntime.getApplicationProperties();
            Properties flinkProperties = applicationProperties.get(APPLICATION_CONFIG_GROUP);
            if (flinkProperties == null) {
                throw new RuntimeException(
                        "Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime.");
            }
            Map<String, String> map = new HashMap<>(flinkProperties.size());
            flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
            return ParameterTool.fromMap(map);
        }
    }

    // Conditionally return execution environment with WebUI
    private static StreamExecutionEnvironment executionEnvironment(String[] args) {
        if (args.length > 0 && Arrays.stream(args).anyMatch("--webUI"::equalsIgnoreCase)) {
            Configuration conf = new Configuration();
            conf.set(
                    ConfigOptions.key("rest.flamegraph.enabled").booleanType().noDefaultValue(),
                    true);
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = executionEnvironment(args);

        final ParameterTool applicationParameters = loadApplicationParameters(args, env);

        if (isLocal(env)) {
            // Checkpointing and parallelism are set by MSF when running on AWS
            // env.enableCheckpointing(60000);
            env.setParallelism(2);
            env.disableOperatorChaining();
        }

        String prometheusRegion = applicationParameters.get("Region");
        String prometheusRemoteWriteUrl = applicationParameters.get("PrometheusWriteUrl");
        int sinkParallelismDivisor = applicationParameters.getInt("SinkParallelismDivisor", 1);

        int generatorMinSamplesPerTimeSeries =
                applicationParameters.getInt("GeneratorMinNrOfSamplesPerTimeSeries", 2);
        int generatorMaxSamplesPerTimeSeries =
                applicationParameters.getInt("GeneratorMaxNrOfSamplesPerTimeSeries", 10);
        int generatorNumberOfDummySources =
                applicationParameters.getInt("GeneratorNumberOfDummySources", 60);
        long generatorPauseBetweenTimeSeriesMs =
                applicationParameters.getLong("GeneratorPauseBetweenTimeSeriesMs", 100);
        int maxRequestRetryCount =
                applicationParameters.getInt("MaxRequestRetryCount", Integer.MAX_VALUE);

        int applicationParallelism = env.getParallelism();
        int generatorParallelism = applicationParallelism;
        int sinkParallelism = applicationParallelism / sinkParallelismDivisor;

        LOGGER.info(
                "Sink connector configuration:"
                        + "\n\t\tSink parallelism: {}"
                        + "\n\t\tPrometheus URL:{}\n\t\tPrometheus Region:{}",
                sinkParallelism,
                prometheusRemoteWriteUrl,
                prometheusRegion);

        LOGGER.info(
                "Data Generator configuration:"
                        + "\n\t\tGenerator parallelism: {}"
                        + "\n\t\tMin samples per time series:{}\n\t\tMax samples per time series:{}\n\t\tPause between time series:{} ms"
                        + "\n\t\tMax request retry count:{}"
                        + "\n\t\tNumber of dummy sources:{} ({} per parallelism)",
                generatorParallelism,
                generatorMinSamplesPerTimeSeries,
                generatorMaxSamplesPerTimeSeries,
                generatorPauseBetweenTimeSeriesMs,
                maxRequestRetryCount,
                generatorNumberOfDummySources,
                generatorNumberOfDummySources / generatorParallelism);

        // Source function that can generates errors
        //        Supplier<PrometheusTimeSeries> eventGenerator = new
        // InstanceMetricsTimeSeriesGenerator(generatorMinSamplesPerTimeSeries,
        // generatorMaxSamplesPerTimeSeries, IntroduceErrors.NO_ERROR).generator();

        // Single-parallelism source function
        //        Supplier<PrometheusTimeSeries> eventGenerator = new
        // SimpleInstanceMetricsTimeSeriesGenerator(generatorMinSamplesPerTimeSeries,
        // generatorMaxSamplesPerTimeSeries, generatorNumberOfDummySources).generator();
        //        SourceFunction<PrometheusTimeSeries> source = new
        // FixedPaceSource<>(PrometheusTimeSeries.class, eventGenerator,
        // generatorPauseBetweenTimeSeriesMs);

        // Parallel source function
        Preconditions.checkArgument(
                generatorNumberOfDummySources % generatorParallelism == 0,
                "The number of dummy sources must be divisible by the parallelism of the generator operator");
        BiFunction<Integer, Integer, PrometheusTimeSeries> eventGenerator =
                new SimpleInstanceMetricsTimeSeriesGenerator(
                                generatorMinSamplesPerTimeSeries,
                                generatorMaxSamplesPerTimeSeries,
                                generatorNumberOfDummySources)
                        .parallelGenerator();
        SourceFunction<PrometheusTimeSeries> source =
                new ParallelFixedPaceSource<>(
                        PrometheusTimeSeries.class,
                        eventGenerator,
                        generatorPauseBetweenTimeSeriesMs);

        DataStream<PrometheusTimeSeries> prometheusTimeSeries =
                env.addSource(source).setParallelism(generatorParallelism);

        // Setting all config, even if default values
        AsyncSinkBase<PrometheusTimeSeries, Types.TimeSeries> sink =
                PrometheusSink.builder()
                        .setMaxBatchSizeInSamples(500)
                        .setMaxRecordSizeInSamples(500)
                        .setMaxTimeInBufferMS(5000)
                        .setRetryConfiguration(
                                RetryConfiguration.builder()
                                        .setInitialRetryDelayMS(30L)
                                        .setMaxRetryDelayMS(5000L)
                                        .setMaxRetryCount(maxRequestRetryCount)
                                        .build())
                        .setSocketTimeoutMs(5000)
                        .setPrometheusRemoteWriteUrl(prometheusRemoteWriteUrl)
                        .setRequestSigner(
                                new AmazonManagedPrometheusWriteRequestSigner(
                                        prometheusRemoteWriteUrl, prometheusRegion))
                        .build();

        prometheusTimeSeries
                .keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector())
                .sinkTo(sink)
                .setParallelism(sinkParallelism);

        env.execute("Prometheus Sink test");
    }
}
