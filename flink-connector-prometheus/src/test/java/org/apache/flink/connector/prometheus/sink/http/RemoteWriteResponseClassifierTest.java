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

package org.apache.flink.connector.prometheus.sink.http;

import org.apache.hc.core5.http.HttpResponse;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.prometheus.sink.http.HttpClientTestUtils.httpResponse;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.FATAL_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.NON_RETRYABLE_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.RETRYABLE_ERROR;
import static org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseType.UNHANDLED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteWriteResponseClassifierTest {
    @Test
    void shouldClassify100AsUnhandled() {
        HttpResponse response = httpResponse(100);
        assertEquals(UNHANDLED, RemoteWriteResponseClassifier.classify(response));
    }

    @Test
    void shouldClassify200AsSuccess() {
        HttpResponse response = httpResponse(200);

        assertEquals(
                RemoteWriteResponseType.SUCCESS, RemoteWriteResponseClassifier.classify(response));
    }

    @Test
    void shouldClassify400AsNonRetryableError() {
        HttpResponse response = httpResponse(400);

        assertEquals(NON_RETRYABLE_ERROR, RemoteWriteResponseClassifier.classify(response));
    }

    @Test
    void shouldClassify403AsFatal() {
        HttpResponse response = httpResponse(403);

        assertEquals(FATAL_ERROR, RemoteWriteResponseClassifier.classify(response));
    }

    @Test
    void shouldClassify404AsFatal() {
        HttpResponse response = httpResponse(404);

        assertEquals(FATAL_ERROR, RemoteWriteResponseClassifier.classify(response));
    }

    @Test
    void shouldClassify429AsRetryableError() {
        HttpResponse response = httpResponse(429);

        assertEquals(RETRYABLE_ERROR, RemoteWriteResponseClassifier.classify(response));
    }

    @Test
    void shouldClassify500AsRetryableError() {
        HttpResponse response = httpResponse(500);

        assertEquals(RETRYABLE_ERROR, RemoteWriteResponseClassifier.classify(response));
    }
}
