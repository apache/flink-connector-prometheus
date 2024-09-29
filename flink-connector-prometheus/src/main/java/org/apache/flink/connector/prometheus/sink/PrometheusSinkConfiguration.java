/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.OnErrorBehavior.DISCARD_AND_CONTINUE;
import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.OnErrorBehavior.FAIL;

/** This class contains configuration classes for different components of the Prometheus sink. */
@PublicEvolving
public class PrometheusSinkConfiguration {

    /**
     * Defines the behaviour when an error is encountered: discard the offending request and
     * continue, or fail, throwing an exception.
     */
    public enum OnErrorBehavior {
        DISCARD_AND_CONTINUE,
        FAIL
    }

    /**
     * Configure the error-handling behavior of the writer, for different types of error. Also
     * defines default behaviors.
     */
    public static class SinkWriterErrorHandlingBehaviorConfiguration implements Serializable {

        public static final OnErrorBehavior ON_MAX_RETRY_EXCEEDED_DEFAULT_BEHAVIOR = FAIL;
        public static final OnErrorBehavior ON_PROMETHEUS_NON_RETRIABLE_ERROR_DEFAULT_BEHAVIOR =
                DISCARD_AND_CONTINUE;

        /** Behaviour when the max retries is exceeded on Prometheus retriable errors. */
        private final OnErrorBehavior onMaxRetryExceeded;

        /** Behaviour when Prometheus Remote-Write respond with a non-retriable error. */
        private final OnErrorBehavior onPrometheusNonRetriableError;

        public SinkWriterErrorHandlingBehaviorConfiguration(
                OnErrorBehavior onMaxRetryExceeded, OnErrorBehavior onPrometheusNonRetriableError) {
            // onPrometheusNonRetriableError cannot be set to FAIL, because it makes impossible for
            // the job to restart from checkpoint (see FLINK-36319).
            // We are retaining the possibility of configuring the behavior on this type of error to
            // allow implementing a different type of behavior.
            Preconditions.checkArgument(
                    onPrometheusNonRetriableError == DISCARD_AND_CONTINUE,
                    "Only DISCARD_AND_CONTINUE is currently supported for onPrometheusNonRetriableError");
            this.onMaxRetryExceeded = onMaxRetryExceeded;
            this.onPrometheusNonRetriableError = onPrometheusNonRetriableError;
        }

        public OnErrorBehavior getOnMaxRetryExceeded() {
            return onMaxRetryExceeded;
        }

        public OnErrorBehavior getOnPrometheusNonRetriableError() {
            return onPrometheusNonRetriableError;
        }

        /** Builder for PrometheusSinkWriterErrorHandlingConfiguration. */
        public static class Builder {
            private OnErrorBehavior onMaxRetryExceeded = null;
            private OnErrorBehavior onPrometheusNonRetriableError = null;

            public Builder() {}

            public Builder onMaxRetryExceeded(OnErrorBehavior onErrorBehavior) {
                this.onMaxRetryExceeded = onErrorBehavior;
                return this;
            }

            public Builder onPrometheusNonRetriableError(OnErrorBehavior onErrorBehavior) {
                this.onPrometheusNonRetriableError = onErrorBehavior;
                return this;
            }

            public SinkWriterErrorHandlingBehaviorConfiguration build() {
                return new SinkWriterErrorHandlingBehaviorConfiguration(
                        Optional.ofNullable(onMaxRetryExceeded)
                                .orElse(ON_MAX_RETRY_EXCEEDED_DEFAULT_BEHAVIOR),
                        Optional.ofNullable(onPrometheusNonRetriableError)
                                .orElse(ON_PROMETHEUS_NON_RETRIABLE_ERROR_DEFAULT_BEHAVIOR));
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final SinkWriterErrorHandlingBehaviorConfiguration DEFAULT_BEHAVIORS =
                builder().build();
    }

    /** Defines the retry strategy configuration. */
    public static class RetryConfiguration implements Serializable {
        public static final long DEFAULT_INITIAL_RETRY_DELAY_MS = 30L;
        public static final long DEFAULT_MAX_RETRY_DELAY_MS = 5000L;
        public static final int DEFAULT_MAX_RETRY_COUNT = 100;

        private final long initialRetryDelayMS;
        private final long maxRetryDelayMS;
        private final int maxRetryCount;

        public long getInitialRetryDelayMS() {
            return initialRetryDelayMS;
        }

        public long getMaxRetryDelayMS() {
            return maxRetryDelayMS;
        }

        public int getMaxRetryCount() {
            return maxRetryCount;
        }

        public RetryConfiguration(
                long initialRetryDelayMS, long maxRetryDelayMS, int maxRetryCount) {
            Preconditions.checkArgument(initialRetryDelayMS > 0, "Initial retry delay must be > 0");
            Preconditions.checkArgument(maxRetryDelayMS > 0, "Max retry delay must be > 0");
            Preconditions.checkArgument(
                    maxRetryDelayMS >= initialRetryDelayMS,
                    "Max retry delay must be >= Initial retry delay");
            Preconditions.checkArgument(maxRetryCount > 0, "Max retry count must be > 0");
            this.initialRetryDelayMS = initialRetryDelayMS;
            this.maxRetryDelayMS = maxRetryDelayMS;
            this.maxRetryCount = maxRetryCount;
        }

        public static Builder builder() {
            return new Builder();
        }

        /** Builder. */
        public static class Builder {
            private long initialRetryDelayMS = DEFAULT_INITIAL_RETRY_DELAY_MS;
            private long maxRetryDelayMS = DEFAULT_MAX_RETRY_DELAY_MS;
            private int maxRetryCount = DEFAULT_MAX_RETRY_COUNT;

            public Builder setInitialRetryDelayMS(long initialRetryDelayMS) {
                this.initialRetryDelayMS = initialRetryDelayMS;
                return this;
            }

            public Builder setMaxRetryDelayMS(long maxRetryDelayMS) {
                this.maxRetryDelayMS = maxRetryDelayMS;
                return this;
            }

            public Builder setMaxRetryCount(int maxRetryCount) {
                this.maxRetryCount = maxRetryCount;
                return this;
            }

            public RetryConfiguration build() {
                return new RetryConfiguration(initialRetryDelayMS, maxRetryDelayMS, maxRetryCount);
            }
        }

        public static final RetryConfiguration DEFAULT_RETRY_CONFIGURATION =
                new RetryConfiguration.Builder().build();
    }
}
