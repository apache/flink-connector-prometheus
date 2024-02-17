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

package org.apache.flink.connector.prometheus.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simple implementation of {@link
 * org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner} that allows captures the
 * headers to be signed and pass through everything unmodified.
 */
public class CapturingPrometheusRequestSigner implements PrometheusRequestSigner {

    private final List<Map<String, String>> requestHeadersList = new ArrayList<>();
    private final List<byte[]> requestBodyList = new ArrayList<>();

    @Override
    public void addSignatureHeaders(Map<String, String> requestHeaders, byte[] requestBody) {
        requestHeadersList.add(requestHeaders);
        requestBodyList.add(requestBody);
    }

    public int getInvocationCount() {
        return requestHeadersList.size();
    }

    public Map<String, String> getRequestHeadersAtInvocationCount(int invocationCount) {
        if (invocationCount <= requestHeadersList.size()) {
            return requestHeadersList.get(invocationCount - 1);
        } else {
            return null;
        }
    }

    public byte[] getRequestBodyAtInvocationCount(int invocationCount) {
        if (invocationCount <= requestBodyList.size()) {
            return requestBodyList.get(invocationCount - 1);
        } else {
            return null;
        }
    }
}
