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

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

/**
 * Dummy implementation of {@link
 * org.apache.flink.connector.prometheus.sink.metrics.SinkMetricsCallback} wrapping dummy metrics,
 * that allows verifying invoked callbacks.
 */
public class VerifybleSinkMetricsCallback extends SinkMetricsCallback {
    private int successfulWriteRequestsCount = 0;
    private int failedWriteRequestForNonRetriableErrorCount = 0;
    private int failedWriteRequestForRetryLimitExceededCount = 0;
    private int failedWriteRequestForHttpClientIoFailCount = 0;
    private int writeRequestsRetryCount = 0;

    public VerifybleSinkMetricsCallback() {
        super(
                SinkMetrics.registerSinkMetrics(
                        UnregisteredMetricsGroup.createSinkWriterMetricGroup()));
    }

    @Override
    public void onSuccessfulWriteRequest(long sampleCount) {
        successfulWriteRequestsCount++;
    }

    @Override
    public void onFailedWriteRequestForNonRetriableError(long sampleCount) {
        failedWriteRequestForNonRetriableErrorCount++;
    }

    @Override
    public void onFailedWriteRequestForRetryLimitExceeded(long sampleCount) {
        failedWriteRequestForRetryLimitExceededCount++;
    }

    @Override
    public void onFailedWriteRequestForHttpClientIoFail(long sampleCount) {
        failedWriteRequestForHttpClientIoFailCount++;
    }

    @Override
    public void onWriteRequestRetry() {
        writeRequestsRetryCount++;
    }

    public boolean verifyOnlySuccessfulWriteRequestsWasCalledOnce() {
        return successfulWriteRequestsCount == 1
                && failedWriteRequestForNonRetriableErrorCount == 0
                && failedWriteRequestForRetryLimitExceededCount == 0
                && failedWriteRequestForHttpClientIoFailCount == 0
                && writeRequestsRetryCount == 0;
    }

    public boolean verifyOnlyFailedWriteRequestsForNonRetriableErrorWasCalledOnce() {
        return successfulWriteRequestsCount == 0
                && failedWriteRequestForNonRetriableErrorCount == 1
                && failedWriteRequestForRetryLimitExceededCount == 0
                && failedWriteRequestForHttpClientIoFailCount == 0
                && writeRequestsRetryCount == 0;
    }

    public boolean verifyOnlyFailedWriteRequestsForRetryLimitExceededWasCalledOnce() {
        return successfulWriteRequestsCount == 0
                && failedWriteRequestForNonRetriableErrorCount == 0
                && failedWriteRequestForRetryLimitExceededCount == 1
                && failedWriteRequestForHttpClientIoFailCount == 0
                && writeRequestsRetryCount == 0;
    }

    public boolean verifyOnlyFailedWriteRequestsForHttpClientIoFailWasCalledOnce() {
        return successfulWriteRequestsCount == 0
                && failedWriteRequestForNonRetriableErrorCount == 0
                && failedWriteRequestForRetryLimitExceededCount == 0
                && failedWriteRequestForHttpClientIoFailCount == 1
                && writeRequestsRetryCount == 0;
    }

    public boolean verifyOnlyWriteRequestsRetryWasCalled(int times) {
        return successfulWriteRequestsCount == 0
                && failedWriteRequestForNonRetriableErrorCount == 0
                && failedWriteRequestForRetryLimitExceededCount == 0
                && failedWriteRequestForHttpClientIoFailCount == 0
                && writeRequestsRetryCount == times;
    }
}
