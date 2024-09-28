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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoteWriteResponseClassifierTest {
    @Test
    void shouldClassify200AsSuccess() {
        HttpResponse response = httpResponse(200);

        assertTrue(RemoteWriteResponseClassifier.isSuccessResponse(response));

        assertFalse(RemoteWriteResponseClassifier.isFatalErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isNonRetriableErrorResponse(response));
    }

    @Test
    void shouldClassify400AsNonRetriableError() {
        HttpResponse response = httpResponse(400);

        assertTrue(RemoteWriteResponseClassifier.isNonRetriableErrorResponse(response));

        assertFalse(RemoteWriteResponseClassifier.isFatalErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isSuccessResponse(response));
    }

    @Test
    void shouldClassify403AsFatal() {
        HttpResponse response = httpResponse(403);

        assertTrue(RemoteWriteResponseClassifier.isFatalErrorResponse(response));

        assertFalse(RemoteWriteResponseClassifier.isNonRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isSuccessResponse(response));
    }

    @Test
    void shouldClassify404AsFatal() {
        HttpResponse response = httpResponse(404);

        assertTrue(RemoteWriteResponseClassifier.isFatalErrorResponse(response));

        assertFalse(RemoteWriteResponseClassifier.isNonRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isSuccessResponse(response));
    }

    @Test
    void shouldClassify429AsRetrialeError() {
        HttpResponse response = httpResponse(429);

        assertTrue(RemoteWriteResponseClassifier.isRetriableErrorResponse(response));

        assertFalse(RemoteWriteResponseClassifier.isNonRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isFatalErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isSuccessResponse(response));
    }

    @Test
    void shouldClassify500AsRetriableError() {
        HttpResponse response = httpResponse(500);

        assertTrue(RemoteWriteResponseClassifier.isRetriableErrorResponse(response));

        assertFalse(RemoteWriteResponseClassifier.isNonRetriableErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isFatalErrorResponse(response));
        assertFalse(RemoteWriteResponseClassifier.isSuccessResponse(response));
    }
}
