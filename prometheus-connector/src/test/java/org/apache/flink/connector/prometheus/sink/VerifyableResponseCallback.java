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

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.concurrent.CallbackContribution;

import java.util.ArrayList;
import java.util.List;

/** Wrapper of {@link HttpResponseCallback} that captures the completion of the Future. */
class VerifyableResponseCallback extends CallbackContribution<SimpleHttpResponse> {

    private final HttpResponseCallback responseCallback;
    private final List<SimpleHttpResponse> completedResponses = new ArrayList<>();

    VerifyableResponseCallback(HttpResponseCallback responseCallback) {
        super(responseCallback);
        this.responseCallback = responseCallback;
    }

    @Override
    public void completed(SimpleHttpResponse response) {
        // Capture the completed response
        completedResponses.add(response);
        // Forward to the wrapped callback
        responseCallback.completed(response);
    }

    public int getCompletedResponsesCount() {
        return completedResponses.size();
    }
}
