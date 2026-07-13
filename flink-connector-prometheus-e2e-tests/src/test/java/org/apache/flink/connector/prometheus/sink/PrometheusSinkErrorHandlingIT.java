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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Integration tests verifying error handling behavior when writing to a containerized Prometheus
 * instance.
 */
@Testcontainers
public class PrometheusSinkErrorHandlingIT {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @Container private static final PrometheusContainer PROMETHEUS = new PrometheusContainer();

    @Test
    void shouldDiscardAndContinueOnMaxRetryExceeded() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        PrometheusTimeSeries timeSeries =
                PrometheusTimeSeries.builder()
                        .withMetricName("error_test_discard_continue")
                        .addLabel("instance", "resilient")
                        .addSample(99.0, System.currentTimeMillis())
                        .build();

        DataStream<PrometheusTimeSeries> stream =
                env.fromCollection(
                        new ArrayList<>(Collections.singletonList(timeSeries)),
                        TypeInformation.of(PrometheusTimeSeries.class));

        PrometheusSink sink =
                (PrometheusSink)
                        PrometheusSink.builder()
                                .setPrometheusRemoteWriteUrl(PROMETHEUS.getRemoteWriteUrl())
                                .setMaxBatchSizeInSamples(500)
                                .setMaxRecordSizeInSamples(500)
                                .setMaxTimeInBufferMS(500)
                                .setRetryConfiguration(
                                        PrometheusSinkConfiguration.RetryConfiguration.builder()
                                                .setInitialRetryDelayMS(100L)
                                                .setMaxRetryDelayMS(1000L)
                                                .setMaxRetryCount(3)
                                                .build())
                                .setSocketTimeoutMs(5000)
                                .setErrorHandlingBehaviorConfiguration(
                                        PrometheusSinkConfiguration
                                                .SinkWriterErrorHandlingBehaviorConfiguration
                                                .builder()
                                                .onMaxRetryExceeded(
                                                        PrometheusSinkConfiguration.OnErrorBehavior
                                                                .DISCARD_AND_CONTINUE)
                                                .onPrometheusNonRetryableError(
                                                        PrometheusSinkConfiguration.OnErrorBehavior
                                                                .DISCARD_AND_CONTINUE)
                                                .build())
                                .build();

        stream.sinkTo(sink);

        // Should complete without throwing - DISCARD_AND_CONTINUE allows the job to finish
        env.execute("PrometheusSinkErrorHandlingIT - discard and continue");
    }
}
