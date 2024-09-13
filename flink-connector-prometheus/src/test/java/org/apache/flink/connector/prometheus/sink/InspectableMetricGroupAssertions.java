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

import org.apache.flink.connector.prometheus.sink.metrics.SinkMetrics;

import org.junit.jupiter.api.Assertions;

/** Assertions to verify whether metrics of a {@link InspectableMetricGroup} has been modified. */
public class InspectableMetricGroupAssertions {
    public static void assertCounterWasIncremented(
            InspectableMetricGroup metricGroup, SinkMetrics.SinkCounter sinkCounter) {
        String counterName = sinkCounter.getMetricName();
        Assertions.assertTrue(
                metricGroup.getCounterCount(counterName) > 0,
                "The counter " + counterName + " has not been incremented");
    }

    public static void assertCounterCount(
            long expectedCounterCount,
            InspectableMetricGroup metricGroup,
            SinkMetrics.SinkCounter sinkCounter) {
        String counterName = sinkCounter.getMetricName();
        long actualCounterCount = metricGroup.getCounterCount(counterName);
        Assertions.assertEquals(
                expectedCounterCount,
                actualCounterCount,
                "The counter "
                        + counterName
                        + " was expected to be "
                        + expectedCounterCount
                        + " but was "
                        + actualCounterCount);
    }

    public static void assertCounterWasNotIncremented(
            InspectableMetricGroup metricGroup, SinkMetrics.SinkCounter sinkCounter) {
        String counterName = sinkCounter.getMetricName();
        Assertions.assertTrue(
                metricGroup.getCounterCount(counterName) == 0,
                "The counter " + counterName + " has been incremented");
    }

    public static void assertCountersWereNotIncremented(
            InspectableMetricGroup metricGroup, SinkMetrics.SinkCounter... counters) {
        for (SinkMetrics.SinkCounter counter : counters) {
            assertCounterWasNotIncremented(metricGroup, counter);
        }
    }
}
