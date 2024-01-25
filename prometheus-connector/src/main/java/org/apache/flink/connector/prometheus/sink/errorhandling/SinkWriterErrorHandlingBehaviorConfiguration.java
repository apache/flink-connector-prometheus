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

package org.apache.flink.connector.prometheus.sink.errorhandling;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.connector.prometheus.sink.errorhandling.OnErrorBehavior.FAIL;

/**
 * Configure the error-handling behavior of the writer, for different types of error. Also defines
 * default behaviors.
 */
public class SinkWriterErrorHandlingBehaviorConfiguration implements Serializable {

    public static final OnErrorBehavior ON_MAX_RETRY_EXCEEDED_DEFAULT_BEHAVIOR = FAIL;
    public static final OnErrorBehavior ON_HTTP_CLIENT_IO_FAIL_DEFAULT_BEHAVIOR = FAIL;
    public static final OnErrorBehavior ON_PROMETHEUS_NON_RETRIABLE_ERROR_DEFAULT_BEHAVIOR = FAIL;

    /** Behaviour when the max retries is exceeded on Prometheus retriable errors. */
    private final OnErrorBehavior onMaxRetryExceeded;

    /** Behaviour when the HTTP client fails, for an I/O problem. */
    private final OnErrorBehavior onHttpClientIOFail;

    /** Behaviour when Prometheus Remote-Write respond with a non-retriable error. */
    private final OnErrorBehavior onPrometheusNonRetriableError;

    public SinkWriterErrorHandlingBehaviorConfiguration(
            OnErrorBehavior onMaxRetryExceeded,
            OnErrorBehavior onHttpClientIOFail,
            OnErrorBehavior onPrometheusNonRetriableError) {
        this.onMaxRetryExceeded = onMaxRetryExceeded;
        this.onHttpClientIOFail = onHttpClientIOFail;
        this.onPrometheusNonRetriableError = onPrometheusNonRetriableError;
    }

    public OnErrorBehavior getOnMaxRetryExceeded() {
        return onMaxRetryExceeded;
    }

    public OnErrorBehavior getOnHttpClientIOFail() {
        return onHttpClientIOFail;
    }

    public OnErrorBehavior getOnPrometheusNonRetriableError() {
        return onPrometheusNonRetriableError;
    }

    /** Builder for PrometheusSinkWriterErrorHandlingConfiguration. */
    public static class Builder {
        private OnErrorBehavior onMaxRetryExceeded = null;
        private OnErrorBehavior onHttpClientIOFail = null;
        private OnErrorBehavior onPrometheusNonRetriableError = null;

        public Builder() {}

        public Builder onMaxRetryExceeded(OnErrorBehavior onErrorBehavior) {
            this.onMaxRetryExceeded = onErrorBehavior;
            return this;
        }

        public Builder onHttpClientIOFail(OnErrorBehavior onErrorBehavior) {
            this.onHttpClientIOFail = onErrorBehavior;
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
                    Optional.ofNullable(onHttpClientIOFail)
                            .orElse(ON_HTTP_CLIENT_IO_FAIL_DEFAULT_BEHAVIOR),
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
