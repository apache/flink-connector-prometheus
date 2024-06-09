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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper of {@link HttpResponseCallback} that captures the completion of the Future, and any
 * exception thrown.
 *
 * <p>Note that any exception thrown by completed() is captured and not rethrown.
 */
public class VerifyableResponseCallback extends CallbackContribution<SimpleHttpResponse> {

    private final HttpResponseCallback responseCallback;
    private final List<SimpleHttpResponse> completedResponses = new ArrayList<>();
    private final Map<Integer, Exception> thrownExceptions = new HashMap<>();

    VerifyableResponseCallback(HttpResponseCallback responseCallback) {
        super(responseCallback);
        this.responseCallback = responseCallback;
    }

    @Override
    public void completed(SimpleHttpResponse response) {
        int thisInvocationCount = completedResponses.size() + 1;

        // Capture the completed response
        completedResponses.add(response);
        // Forward to the wrapped callback, capturing any exception
        try {
            responseCallback.completed(response);
        } catch (Exception ex) {
            thrownExceptions.put(thisInvocationCount, ex);
        }
    }

    public int getCompletedResponsesCount() {
        return completedResponses.size();
    }

    public Exception getThrownExceptionAtInvocationCount(int invocationCount) {
        if (invocationCount <= getCompletedResponsesCount()) {
            return thrownExceptions.get(invocationCount);
        } else {
            return null;
        }
    }

    public SimpleHttpResponse getCompletedResponseAtInvocationCount(int invocationCount) {
        if (invocationCount <= getCompletedResponsesCount()) {
            return completedResponses.get(invocationCount - 1);
        } else {
            return null;
        }
    }
}
