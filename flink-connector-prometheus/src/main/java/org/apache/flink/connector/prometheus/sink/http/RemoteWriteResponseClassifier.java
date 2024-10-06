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

import org.apache.flink.annotation.Internal;

import org.apache.hc.core5.http.HttpResponse;

import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.FATAL_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.NON_RETRIABLE_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.RETRIABLE_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.SUCCESS;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.UNHANDLED;

/** Classify http responses based on the status code. */
@Internal
public class RemoteWriteResponseClassifier {

    public static RemoteWriteResponseType classify(HttpResponse response) {
        int statusCode = response.getCode();
        if (statusCode >= 200 && statusCode < 300) {
            // 2xx: success
            return SUCCESS;
        } else if (statusCode == 429) {
            // 429, Too Many Requests: throttling
            return RETRIABLE_ERROR;
        } else if (statusCode == 403 || statusCode == 404) {
            // 403, Forbidden: authentication error
            // 404, Not Found: wrong endpoint URL path
            return FATAL_ERROR;
        } else if (statusCode >= 400 && statusCode < 500) {
            // 4xx (except 403, 404, 429): wrong request/bad data
            return NON_RETRIABLE_ERROR;
        } else if (statusCode >= 500) {
            // 5xx: internal errors, recoverable
            return RETRIABLE_ERROR;
        } else {
            // Other status code are unhandled
            return UNHANDLED;
        }
    }
}
