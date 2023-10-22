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
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InstanceSinkCounterPrometheusTimeSeriesGeneratorTest {

    @Test
    public void duplicateSampleTimestampInSameTimeSeries() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.DUPLICATE_SAMPLE_TIMESTAMPS_IN_THE_SAME_REQUEST)
                .generator();

        var timeSeries = generator.get();

        Set<Long> timeStamps = new HashSet<>();
        boolean expectedError = false;
        for (PrometheusTimeSeries.Sample sample : timeSeries.getSamples()) {
            if (timeStamps.contains(sample.getTimestamp())) {
                expectedError = true;
                break;
            }
            timeStamps.add(sample.getTimestamp());
        }

        assertTrue(expectedError, "Duplicate Timestamp not found");
    }

    @Test
    public void duplicateSampleInSameTimeSeries() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.DUPLICATE_SAMPLES_IN_THE_SAME_REQUEST)
                .generator();

        var timeSeries = generator.get();

        Map<Long, Double> samples = new HashMap<>();
        boolean expectedError = false;
        for (PrometheusTimeSeries.Sample sample : timeSeries.getSamples()) {
            Long timestamp = sample.getTimestamp();
            Double value = sample.getValue();
            if (samples.containsKey(timestamp) && samples.get(timestamp).equals(value)) {
                expectedError = true;
                break;
            }
            samples.put(timestamp, sample.getValue());
        }

        assertTrue(expectedError, "Duplicate Sample not found");
    }

    @Test
    public void outOfOrderSamplesInSameTimeSeries() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.OUT_OF_ORDER_SAMPLES_IN_THE_SAME_TIME_SERIES)
                .generator();

        var timeSeries = generator.get();
        Long prevSampleTimestamp = null;
        boolean expectedError = false;
        for (PrometheusTimeSeries.Sample sample : timeSeries.getSamples()) {
            if (prevSampleTimestamp != null && prevSampleTimestamp > sample.getTimestamp()) {
                expectedError = true;
                break;
            }
            prevSampleTimestamp = sample.getTimestamp();
        }

        assertTrue(expectedError, "No out-of-order Sample found");
    }

    @Test
    public void outOfOrderSamplesAcrossTimeSeries() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.OUT_OF_ORDER_SAMPLES_ACROSS_TIME_SERIES)
                .generator();

        List<PrometheusTimeSeries> timeSeriesList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            timeSeriesList.add(generator.get());
        }

        Long prevSampleTimestamp = null;
        boolean expectedError = false;
        outerloop:
        for (PrometheusTimeSeries timeSeries : timeSeriesList) {
            for (PrometheusTimeSeries.Sample sample : timeSeries.getSamples()) {
                if (prevSampleTimestamp != null && prevSampleTimestamp > sample.getTimestamp()) {
                    expectedError = true;
                    break outerloop;
                }
                prevSampleTimestamp = sample.getTimestamp();
            }
        }
        assertTrue(expectedError, "No out-of-order Sample found");
    }

    @Test
    public void duplicateLabelName() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.DUPLICATE_LABEL_NAMES)
                .generator();

        var timeSeries = generator.get();

        Set<String> labelNames = new HashSet<>();
        boolean expectedError = false;
        for (PrometheusTimeSeries.Label label : timeSeries.getLabels()) {
            if (labelNames.contains(label.getName())) {
                expectedError = true;
                break;
            }
            labelNames.add(label.getName());
        }

        assertTrue(expectedError, "Duplicate Label name not found");
    }

    @Test
    public void outOfOrderLabelNames() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.OUT_OF_ORDER_LABEL_NAMES)
                .generator();

        var timeSeries = generator.get();

        String prevLabel = null;
        boolean expectedError = false;
        for (PrometheusTimeSeries.Label label : timeSeries.getLabels()) {
            if (prevLabel != null && prevLabel.compareTo(label.getName()) > 0) {
                expectedError = true;
                break;
            }
            prevLabel = label.getName();
        }

        assertTrue(expectedError, "No out-of-order Label name found");
    }

    @Test
    public void illegalLabelName() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.ILLEGAL_LABEL_NAME)
                .generator();

        var timeSeries = generator.get();
        boolean expectedError = false;
        String validLabelNamePatter = "[a-zA-Z_]([a-zA-Z0-9_])*";
        for (PrometheusTimeSeries.Label label : timeSeries.getLabels()) {
            String labelName = label.getName();
            if (!labelName.matches(validLabelNamePatter) || labelName.startsWith("__")) {
                expectedError = true;
                break;
            }
        }
        assertTrue(expectedError, "No illegal Label name found");
    }

    @Test
    public void illegalMetricName() {
        var generator = new InstanceMetricsTimeSeriesGenerator(4, 4,
                InstanceMetricsTimeSeriesGenerator.IntroduceErrors.ILLEGAL_METRIC_NAME)
                .generator();

        var timeSeries = generator.get();

        String validMetricName = "[a-zA-Z_]([a-zA-Z0-9_])*";
        String metricName = "";
        for (PrometheusTimeSeries.Label label : timeSeries.getLabels()) {
            if (label.getName().equals("__name__")) {
                metricName = label.getName();
                break;
            }
        }

        assertFalse(metricName.matches(validMetricName), "Metric name should be illegal");
    }
}