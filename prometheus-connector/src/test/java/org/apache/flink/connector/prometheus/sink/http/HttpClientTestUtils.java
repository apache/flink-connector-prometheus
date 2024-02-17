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

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.message.BasicHttpRequest;
import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.apache.hc.core5.http.protocol.BasicHttpContext;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test utilities for testing features involving the Apache Http Client. */
public class HttpClientTestUtils {

    public static FutureCallback<SimpleHttpResponse> statusCodeAsserter(int expectedStatusCode) {

        return new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                assertEquals(
                        expectedStatusCode,
                        response.getCode(),
                        "Request should return status code " + expectedStatusCode);
            }

            @Override
            public void failed(Exception ex) {
                Assertions.fail("Request should not throw exception");
            }

            @Override
            public void cancelled() {
                Assertions.fail("Request should not be cancelled");
            }
        };
    }

    public static FutureCallback<SimpleHttpResponse> loggingCallback(Logger logger) {
        return new FutureCallback<>() {
            @Override
            public void completed(SimpleHttpResponse simpleHttpResponse) {
                logger.info(
                        "Request Success: {},{}",
                        simpleHttpResponse.getCode(),
                        simpleHttpResponse.getReasonPhrase());
            }

            @Override
            public void failed(Exception e) {
                logger.info("Request Failure", e);
            }

            @Override
            public void cancelled() {
                logger.info("Request Cancelled");
            }
        };
    }

    public static HttpContext httpContext() {
        return new BasicHttpContext();
    }

    public static HttpRequest postHttpRequest() {
        return new BasicHttpRequest(Method.POST, "/");
    }

    public static HttpResponse httpResponse(int statusCode) {
        return new BasicHttpResponse(statusCode);
    }
}
