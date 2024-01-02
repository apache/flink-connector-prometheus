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

package com.amazonaws.services.managedflink.domain;

import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Very simple dummy TimeSeries generator, generating random `CPU` and `Memory` samples for a given
 * number of instances.
 *
 * <p>Sample timestamp is always the current system time. The value is random, between 0 and 1
 */
public class SimpleInstanceMetricsTimeSeriesGenerator implements Serializable {

    private final int numberOfDummyInstances;
    private final int minNrOfSamples;
    private final int maxNrOfSamples;

    public SimpleInstanceMetricsTimeSeriesGenerator(
            int minNrOfSamples, int maxNrOfSamples, int numberOfDummyInstances) {
        this.numberOfDummyInstances = numberOfDummyInstances;
        this.minNrOfSamples = minNrOfSamples;
        this.maxNrOfSamples = maxNrOfSamples;
    }

    private String dummyInstanceId(int number) {
        return "I" + String.format("%010d", number);
    }

    private PrometheusTimeSeries nextTimeSeries(int minNrOfSamples, int maxNrOfSamples) {
        return nextTimeSeries(minNrOfSamples, maxNrOfSamples, 1, 0);
    }

    private PrometheusTimeSeries nextTimeSeries(
            int minNrOfSamples,
            int maxNrOfSamples,
            int numberOfParallelSubTasks,
            int subTaskIndex) {
        int sourcesPerSubTask = numberOfDummyInstances / numberOfParallelSubTasks;
        int dummySourceIndex =
                sourcesPerSubTask * subTaskIndex + RandomUtils.nextInt(0, sourcesPerSubTask);
        String instanceId = dummyInstanceId(dummySourceIndex);

        int nrOfSamples = RandomUtils.nextInt(minNrOfSamples, maxNrOfSamples + 1);
        String metricName = (RandomUtils.nextDouble(0, 1) < 0.5) ? "CPU" : "Memory";

        PrometheusTimeSeries.Builder builder =
                PrometheusTimeSeries.builder()
                        .withMetricName(metricName)
                        .addLabel("InstanceId", instanceId);
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

    public BiFunction<Integer, Integer, PrometheusTimeSeries> parallelGenerator() {
        return (BiFunction<Integer, Integer, PrometheusTimeSeries> & Serializable)
                (numberOfParallelSubTasks, subTaskIndex) ->
                        nextTimeSeries(
                                minNrOfSamples,
                                maxNrOfSamples,
                                numberOfParallelSubTasks,
                                subTaskIndex);
    }
}
