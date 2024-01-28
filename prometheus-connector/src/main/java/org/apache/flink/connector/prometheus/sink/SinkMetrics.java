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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** Wraps all metrics in a single class. */
public class SinkMetrics {
    private final Counter[] counters;

    private SinkMetrics(Counter[] counters) {
        this.counters = counters;
    }

    public void inc(SinkCounter counter, long value) {
        counters[counter.ordinal()].inc(value);
    }

    public void inc(SinkCounter counter) {
        counters[counter.ordinal()].inc();
    }

    /** Register all custom sink metrics and return an of this wrapper class. */
    public static SinkMetrics registerSinkMetrics(MetricGroup metrics) {
        // Register all counters
        Counter[] counters = new Counter[SinkCounter.values().length];
        for (SinkCounter metric : SinkCounter.values()) {
            counters[metric.ordinal()] = metrics.counter(metric.getMetricName());
        }
        return new SinkMetrics(counters);
    }

    /** Enum defining all sink counters. */
    public enum SinkCounter {
        // Total number of Samples that were dropped because of not being retriable errors in
        // Prometheus
        NUM_SAMPLES_NON_RETRIABLE_DROPPED("numSamplesNonRetriableDropped"),

        // Number of Samples dropped after reaching retry limit on retriable errors
        NUM_SAMPLES_RETRY_LIMIT_DROPPED("numSamplesRetryLimitDropped"),

        // Total number of Samples dropped due to any reasons: retriable errors reaching retry
        // limit, non-retriable errors, unexpected IO errors
        NUM_SAMPLES_DROPPED("numSamplesDropped"),

        // Number of Samples successfully written to Prometheus
        NUM_SAMPLES_OUT("numSamplesOut"),

        // Number of WriteRequests successfully sent to Prometheus
        NUM_WRITE_REQUESTS_OUT("numWriteRequestsOut"),

        // Number of permanently failed WriteRequests
        NUM_WRITE_REQUESTS_PERMANENTLY_FAILED("numWriteRequestsPermanentlyFailed"),

        // Number of WriteRequests retries
        NUM_WRITE_REQUESTS_RETRIES("numWriteRequestsRetries");

        private final String metricName;

        public String getMetricName() {
            return metricName;
        }

        SinkCounter(String metricName) {
            this.metricName = metricName;
        }
    }
}
