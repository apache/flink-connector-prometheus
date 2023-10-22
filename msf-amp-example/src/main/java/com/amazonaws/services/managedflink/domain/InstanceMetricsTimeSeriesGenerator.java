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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Supplier;

import static com.amazonaws.services.managedflink.domain.InstanceMetricsTimeSeriesGenerator.IntroduceErrors.*;

/**
 * This random data generator is able to introduce some errors that should cause
 * Prometheus to reject the data.
 *
 * LIMITATION: due to the way it generates timestamps, if the interval between subsequent calls to the generator (in millis)
 * is less than the max number of Samples in each generated TimeSeries, timestamps may be out of order in the output
 * stream, and rejected by prometheus
 */
public class InstanceMetricsTimeSeriesGenerator implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceMetricsTimeSeriesGenerator.class);

    public enum IntroduceErrors {
        NO_ERROR,
        DUPLICATE_SAMPLE_TIMESTAMPS_IN_THE_SAME_REQUEST,
        DUPLICATE_SAMPLES_IN_THE_SAME_REQUEST,
        DUPLICATE_LABEL_NAMES,
        OUT_OF_ORDER_LABEL_NAMES,
        OUT_OF_ORDER_SAMPLES_IN_THE_SAME_TIME_SERIES,
        OUT_OF_ORDER_SAMPLES_ACROSS_TIME_SERIES,
        ILLEGAL_LABEL_NAME,
        ILLEGAL_METRIC_NAME,
        TIMESTAMP_TOO_OLD
    }

    private final int minNrOfSamples;
    private final int maxNrOfSamples;
    private final IntroduceErrors introduceErrors;

    private volatile Long prevTimeSeriesLastTimestamp = null;


    public InstanceMetricsTimeSeriesGenerator(int minNrOfSamples, int maxNrOfSamples, IntroduceErrors introduceErrors) {
        this.minNrOfSamples = minNrOfSamples;
        this.maxNrOfSamples = maxNrOfSamples;
        this.introduceErrors = introduceErrors;
    }


    private static PrometheusTimeSeries nextTimeSeries(
            int minNrOfSamples, int maxNrOfSamples,
            IntroduceErrors introduceErrors, Long prevRequestTimestamp) {
        var builder = PrometheusTimeSeries.builder();

        String metricName = (RandomUtils.nextDouble(0, 1) < 0.5) ? "CPU" : "Memory";
        if (introduceErrors == ILLEGAL_METRIC_NAME) {
            // Replace the metricName with an illegal string (not [a-zA-Z_]([a-zA-Z0-9_])* )
            metricName = "$$IllegalMetricName$$";
            LOG.debug("Introducing illegal metric name");
        }
        builder.withMetricName(metricName);

        if (introduceErrors == OUT_OF_ORDER_LABEL_NAMES) {
            // Add a label named "zzzOutOfOrder" before "InstanceId"
            builder.addLabel("zzzOutOfOrder", "whatever");
            LOG.debug("Introducing out-of-order label name");
        } else if (introduceErrors == ILLEGAL_LABEL_NAME) {
            // Add a label with an illegal name (not [a-zA-Z_]([a-zA-Z0-9_])*)
            builder.addLabel("@@IllegalLabel@@", "whatever");
            LOG.debug("Introducing illegal label name");
        }

        String instanceId = RandomStringUtils.randomAlphabetic(2);
        builder.addLabel("InstanceId", instanceId);

        if (introduceErrors == DUPLICATE_LABEL_NAMES) {
            // Add a duplicate label named "InstanceId"
            builder.addLabel("InstanceId", "duplicate-" + instanceId);
            LOG.debug("Introducing duplicate label name");
        }

        long baseTimestamp;
        if (introduceErrors == OUT_OF_ORDER_SAMPLES_ACROSS_TIME_SERIES && prevRequestTimestamp != null) {
            baseTimestamp = prevRequestTimestamp - 60_000L; // to generate ouf of order samples, starts 1 min BEFORE the previous request
        } else if (introduceErrors == TIMESTAMP_TOO_OLD) {
            baseTimestamp = 1L;
        } else {
            baseTimestamp = System.currentTimeMillis();
        }

        int nrOfSamples = RandomUtils.nextInt(minNrOfSamples, maxNrOfSamples + 1);
        long prevSampleTimestamp = 0L;
        double prevValue = Double.MIN_VALUE;
        for (int i = 0; i < nrOfSamples; i++) {
            double value = RandomUtils.nextDouble(0, 1);
            long timestamp = baseTimestamp + i;

            if (introduceErrors == DUPLICATE_SAMPLES_IN_THE_SAME_REQUEST && (i % 2 == 1)) {
                // Introduce a duplicate sample (value and timestamp) every other sample
                value = prevValue;
                timestamp = prevSampleTimestamp;
                LOG.debug("Introducing duplicate sample (timestamp and value)");
            } else if (introduceErrors == DUPLICATE_SAMPLE_TIMESTAMPS_IN_THE_SAME_REQUEST && (i == 1)) {
                // Introduce a duplicate time at the second sample of the timestamp
                timestamp = prevSampleTimestamp;
                LOG.debug("Introducing duplicate sample timestamp");
            } else if (introduceErrors == OUT_OF_ORDER_SAMPLES_IN_THE_SAME_TIME_SERIES && prevSampleTimestamp > 0) {
                timestamp = prevSampleTimestamp - 1; // Force sample timestamp to go backward
                LOG.debug("Introducing out-of-order sample timestamps");
            } else if (timestamp <= prevSampleTimestamp) {
                timestamp = prevSampleTimestamp + 1; // Make sure timestamps are always monotonically increasing
            }
            builder.addSample(value, timestamp);
            prevSampleTimestamp = timestamp;
            prevValue = value;
        }

        var timeSeries = builder.build();
        return timeSeries;
    }

    public Supplier<PrometheusTimeSeries> generator() {
        return (Supplier<PrometheusTimeSeries> & Serializable) () -> {
            PrometheusTimeSeries timeSeries = nextTimeSeries(minNrOfSamples, maxNrOfSamples, introduceErrors, prevTimeSeriesLastTimestamp);
            prevTimeSeriesLastTimestamp = timeSeries.getSamples()[timeSeries.getSamples().length - 1].getTimestamp();
            return timeSeries;
        };
    }

}
