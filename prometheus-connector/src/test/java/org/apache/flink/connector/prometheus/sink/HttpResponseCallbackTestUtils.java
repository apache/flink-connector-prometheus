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

import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Utilities for test involving the {@link
 * org.apache.flink.connector.prometheus.sink.HttpResponseCallback}.
 */
public class HttpResponseCallbackTestUtils {
    public static Consumer<List<Types.TimeSeries>> getRequestResult(
            List<Types.TimeSeries> requeuedResults) {
        return requeuedResults::addAll;
    }

    public static void assertNoReQueuedResult(List<Types.TimeSeries> emittedResults) {
        assertTrue(
                emittedResults.isEmpty(),
                emittedResults.size() + " results were re-queued, but none was expected");
    }

    public static void assertCallbackCompletedOnceWithNoException(
            VerifyableResponseCallback callback) {
        int actualCompletionCount = callback.getCompletedResponsesCount();
        assertEquals(
                1,
                actualCompletionCount,
                "The callback was completed "
                        + actualCompletionCount
                        + " times, but once was expected");

        Exception exceptionThrown = callback.getThrownExceptionAtInvocationCount(1);
        assertNull(exceptionThrown, "An exception was thrown on completed, but none was expected");
    }

    public static void assertCallbackCompletedOnceWithException(
            Class<? extends Exception> expectedExceptionClass,
            VerifyableResponseCallback callback) {
        Exception thrownException = callback.getThrownExceptionAtInvocationCount(1);
        Assertions.assertNotNull(
                thrownException, "Exception on complete was expected, but none was thrown");
        assertTrue(
                thrownException.getClass().isAssignableFrom(expectedExceptionClass),
                "Unexpected exception type thrown on completed");
    }
}
