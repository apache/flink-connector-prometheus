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

/** Exception during writing to Prometheus Remote-Write endpoint. */
public class PrometheusSinkWriteException extends RuntimeException {

    private final String reason;
    private final int httpStatusCode;
    private final String httpReasonPhrase;
    private final int timeSeriesCount;
    private final long sampleCount;
    private final String httpResponseBody;

    public PrometheusSinkWriteException(String reason, int timeSeriesCount, long sampleCount) {
        super("Reason: " + reason);
        this.reason = reason;
        this.timeSeriesCount = timeSeriesCount;
        this.sampleCount = sampleCount;
        this.httpStatusCode = -1;
        this.httpReasonPhrase = "";
        this.httpResponseBody = "";
    }

    public PrometheusSinkWriteException(
            String reason, int timeSeriesCount, long sampleCount, Exception cause) {
        super("Reason: " + reason, cause);
        this.reason = reason;
        this.timeSeriesCount = timeSeriesCount;
        this.sampleCount = sampleCount;
        this.httpStatusCode = -1;
        this.httpReasonPhrase = "";
        this.httpResponseBody = "";
    }

    public PrometheusSinkWriteException(
            String reason,
            int httpStatusCode,
            String httpReasonPhrase,
            int timeSeriesCount,
            long sampleCount,
            String httpResponseBody) {
        super(
                String.format(
                        "Reason: %s. Http response: %d,%s (%s) .The offending write-request contains %d time-series and %d samples",
                        reason,
                        httpStatusCode,
                        httpReasonPhrase,
                        httpResponseBody,
                        timeSeriesCount,
                        sampleCount));
        this.reason = reason;
        this.httpStatusCode = httpStatusCode;
        this.httpReasonPhrase = httpReasonPhrase;
        this.timeSeriesCount = timeSeriesCount;
        this.sampleCount = sampleCount;
        this.httpResponseBody = httpResponseBody;
    }

    public String getReason() {
        return reason;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public String getHttpReasonPhrase() {
        return httpReasonPhrase;
    }

    public int getTimeSeriesCount() {
        return timeSeriesCount;
    }

    public long getSampleCount() {
        return sampleCount;
    }

    public String getHttpResponseBody() {
        return httpResponseBody;
    }
}
