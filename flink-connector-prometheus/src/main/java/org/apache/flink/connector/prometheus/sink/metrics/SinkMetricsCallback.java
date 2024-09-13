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

import org.apache.flink.annotation.Internal;

import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_DROPPED;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_NON_RETRIABLE_DROPPED;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_OUT;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_SAMPLES_RETRY_LIMIT_DROPPED;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_WRITE_REQUESTS_OUT;
import static org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics.SinkCounter.NUM_WRITE_REQUESTS_PERMANENTLY_FAILED;

/** Callback updating {@link SinkMetrics} on specific request outcomes. */
@Internal
public class SinkMetricsCallback {

    private final SinkMetrics metrics;

    /**
     * Instantiate a callback passing the collection of metrics to mutate.
     *
     * @param metrics collection of metrics that will be mutated
     */
    public SinkMetricsCallback(SinkMetrics metrics) {
        this.metrics = metrics;
    }

    private void onFailedWriteRequest(long sampleCount) {
        metrics.inc(NUM_SAMPLES_DROPPED, sampleCount);
        metrics.inc(NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);
    }

    public void onSuccessfulWriteRequest(long sampleCount) {
        metrics.inc(NUM_SAMPLES_OUT, sampleCount);
        metrics.inc(NUM_WRITE_REQUESTS_OUT);
    }

    public void onFailedWriteRequestForNonRetriableError(long sampleCount) {
        metrics.inc(NUM_SAMPLES_NON_RETRIABLE_DROPPED, sampleCount);
        onFailedWriteRequest(sampleCount);
    }

    public void onFailedWriteRequestForRetryLimitExceeded(long sampleCount) {
        metrics.inc(NUM_SAMPLES_RETRY_LIMIT_DROPPED, sampleCount);
        onFailedWriteRequest(sampleCount);
    }

    public void onFailedWriteRequestForHttpClientIoFail(long sampleCount) {
        onFailedWriteRequest(sampleCount);
    }

    public void onWriteRequestRetry() {
        metrics.inc(SinkMetrics.SinkCounter.NUM_WRITE_REQUESTS_RETRIES);
    }
}
