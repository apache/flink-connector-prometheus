/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink.metrics;

import org.apache.flink.connector.prometheus.sink.InspectableMetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.prometheus.sink.InspectableMetricGroupAssertions.assertCounterCount;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_DROPPED;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_NON_RETRYABLE_DROPPED;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_OUT;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_RETRY_LIMIT_DROPPED;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_WRITE_REQUESTS_OUT;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_WRITE_REQUESTS_PERMANENTLY_FAILED;

class SinkMetricsCallbackTest {
    private InspectableMetricGroup metricGroup;
    private SinkMetricsCallback metricsCallback;

    private static final long SAMPLE_COUNT = 42;

    @BeforeEach
    void setUp() {
        metricGroup = new InspectableMetricGroup();
        metricsCallback = new SinkMetricsCallback(SinkMetrics.registerSinkMetrics(metricGroup));
    }

    @Test
    void onSuccessfulWriteRequest() {
        metricsCallback.onSuccessfulWriteRequest(SAMPLE_COUNT);

        assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_OUT);
        assertCounterCount(1, metricGroup, NUM_WRITE_REQUESTS_OUT);

        assertCounterCount(0, metricGroup, NUM_SAMPLES_DROPPED);
        assertCounterCount(0, metricGroup, NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);
    }

    @Test
    void onFailedWriteRequestForNonRetryableError() {
        metricsCallback.onFailedWriteRequestForNonRetryableError(SAMPLE_COUNT);

        assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_NON_RETRYABLE_DROPPED);
        assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_DROPPED);
        assertCounterCount(1, metricGroup, NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);

        assertCounterCount(0, metricGroup, NUM_SAMPLES_OUT);
        assertCounterCount(0, metricGroup, NUM_WRITE_REQUESTS_OUT);
    }

    @Test
    void onFailedWriteRequestForRetryLimitExceeded() {
        metricsCallback.onFailedWriteRequestForRetryLimitExceeded(SAMPLE_COUNT);

        assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_RETRY_LIMIT_DROPPED);
        assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_DROPPED);
        assertCounterCount(1, metricGroup, NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);

        assertCounterCount(0, metricGroup, NUM_SAMPLES_OUT);
        assertCounterCount(0, metricGroup, NUM_WRITE_REQUESTS_OUT);
    }

    @Test
    void onFailedWriteRequestForHttpClientIoFail() {
        metricsCallback.onFailedWriteRequestForHttpClientIoFail(SAMPLE_COUNT);
        assertCounterCount(SAMPLE_COUNT, metricGroup, NUM_SAMPLES_DROPPED);
        assertCounterCount(1, metricGroup, NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);

        assertCounterCount(0, metricGroup, NUM_SAMPLES_OUT);
        assertCounterCount(0, metricGroup, NUM_WRITE_REQUESTS_OUT);
    }
}
