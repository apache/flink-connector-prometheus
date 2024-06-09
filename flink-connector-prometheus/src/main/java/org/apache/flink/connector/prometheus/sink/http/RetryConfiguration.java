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

package org.apache.flink.connector.prometheus.sink.http;

import java.io.Serializable;

/** Defines the retry strategy configuration. */
public class RetryConfiguration implements Serializable {
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

    public RetryConfiguration(long initialRetryDelayMS, long maxRetryDelayMS, int maxRetryCount) {
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
}
