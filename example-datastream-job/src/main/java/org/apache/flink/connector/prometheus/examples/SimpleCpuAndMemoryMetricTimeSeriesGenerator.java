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

import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Very simple dummy TimeSeries generator, generating random `CPU` and `Memory` samples for a given
 * number of sources.
 *
 * <p>Sample timestamp is always the current system time. The value is random, between 0 and 1
 */
public class SimpleCpuAndMemoryMetricTimeSeriesGenerator implements Serializable {

    private final int numberOfSources;
    private final int minNrOfSamples;
    private final int maxNrOfSamples;
    private final short numberOfMetricsPerSource;

    public SimpleCpuAndMemoryMetricTimeSeriesGenerator(
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
