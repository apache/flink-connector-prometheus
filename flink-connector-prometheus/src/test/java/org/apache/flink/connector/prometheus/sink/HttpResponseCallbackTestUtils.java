package org.apache.flink.connector.prometheus.sink;

import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpResponseCallbackTestUtils {
    public static ResultHandler<Types.TimeSeries> getResultHandler() {
        return new CapturingResultHandler();
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

        // Check that resultHandler was NOT completed exceptionally
        assertFalse(
                callback.getCapturingResultHandler().isCompletedExceptionally(),
                "ResultHandler received an exception, but none was expected");
    }

    public static void assertCallbackCompletedOnceWithException(
            Class<? extends Exception> expectedExceptionClass,
            VerifyableResponseCallback callback) {
        // Check that the resultHandler received an exception
        assertTrue(
                callback.getCapturingResultHandler().isCompletedExceptionally(),
                "Exception was expected, but ResultHandler did not receive one");
        Exception receivedException = callback.getCapturingResultHandler().getException();
        Assertions.assertNotNull(
                receivedException, "Exception on complete was expected, but none was received");
        assertTrue(
                expectedExceptionClass.isAssignableFrom(receivedException.getClass()),
                "Unexpected exception type: expected "
                        + expectedExceptionClass.getName()
                        + " but got "
                        + receivedException.getClass().getName());
    }

    public static class CapturingResultHandler implements ResultHandler<Types.TimeSeries> {
        private boolean completed = false;
        private Exception exception = null;
        private final List<Types.TimeSeries> retriedEntries = new ArrayList<>();

        @Override
        public void complete() {
            completed = true;
        }

        @Override
        public void completeExceptionally(Exception e) {
            exception = e;
        }

        @Override
        public void retryForEntries(List<Types.TimeSeries> entries) {
            retriedEntries.addAll(entries);
        }

        public boolean isCompleted() {
            return completed;
        }

        public boolean isCompletedExceptionally() {
            return exception != null;
        }

        public Exception getException() {
            return exception;
        }

        public List<Types.TimeSeries> getRetriedEntries() {
            return retriedEntries;
        }
    }
}
