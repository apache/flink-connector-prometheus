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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrometheusTimeSeriesTest {

    private static PrometheusTimeSeries aTimeSeries() {
        return PrometheusTimeSeries.builder()
                .withMetricName("MyMetricName")
                .addLabel("Label1", "Value1")
                .addLabel("Label2", "Value2")
                .addSample(0.1, 1L)
                .addSample(0.2, 2L)
                .build();
    }

    private static PrometheusTimeSeries aDifferentTimeSeries() {
        return PrometheusTimeSeries.builder()
                .withMetricName("AnotherMetricName")
                .addLabel("Label3", "Value3")
                .addLabel("Label4", "Value4")
                .addSample(0.2, 3L)
                .addSample(0.7, 4L)
                .build();
    }

    @Test
    public void testEqualsReflexivity() {
        PrometheusTimeSeries ts1 = aTimeSeries();

        assertTrue(ts1.equals(ts1));
    }

    @Test
    public void testEqualsSymmetry() {
        PrometheusTimeSeries ts1 = aTimeSeries();
        PrometheusTimeSeries ts2 = aTimeSeries();

        assertTrue(ts1.equals(ts2) && ts2.equals(ts1));
    }

    @Test
    public void testEqualsTransitivity() {
        PrometheusTimeSeries ts1 = aTimeSeries();
        PrometheusTimeSeries ts2 = aTimeSeries();
        PrometheusTimeSeries ts3 = aTimeSeries();

        assertTrue(ts1.equals(ts2) && ts2.equals(ts3) && ts1.equals(ts3));
    }

    @Test
    public void testEqualsConsistency() {
        PrometheusTimeSeries ts1 = aTimeSeries();
        PrometheusTimeSeries ts2 = aTimeSeries();

        assertTrue(ts1.equals(ts2) == ts1.equals(ts2));
    }

    @Test
    public void testEqualsNullComparison() {
        PrometheusTimeSeries ts1 = aTimeSeries();

        assertFalse(ts1.equals(null));
    }

    @Test
    public void testHashCodeConsistency() {
        PrometheusTimeSeries ts1 = aTimeSeries();
        int hashCode = ts1.hashCode();

        assertEquals(hashCode, ts1.hashCode());
    }

    @Test
    public void testEqualObjectsHaveEqualHashCodes() {
        PrometheusTimeSeries ts1 = aTimeSeries();
        PrometheusTimeSeries ts2 = aTimeSeries();

        assertTrue(ts1.equals(ts2));
        assertEquals(ts1.hashCode(), ts2.hashCode());
    }

    @Test
    public void testUnequalObjectsHaveDifferentHashCodes() {
        PrometheusTimeSeries ts1 = aTimeSeries();
        PrometheusTimeSeries ts2 = aDifferentTimeSeries();

        assertFalse(ts1.equals(ts2));
        assertNotEquals(ts1.hashCode(), ts2.hashCode());
    }

    @Test
    public void testBuilder() {

        PrometheusTimeSeries ts =
                PrometheusTimeSeries.builder()
                        .withMetricName("MyMetricName")
                        .addLabel("Label1", "Value1")
                        .addLabel("Label2", "Value2")
                        .addSample(0.1, 1L)
                        .addSample(0.2, 2L)
                        .addSample(0.3, 3L)
                        .build();

        assertEquals("MyMetricName", ts.getMetricName());
        assertEquals(2, ts.getLabels().length);
        assertLabelMatches("Label1", "Value1", ts.getLabels()[0]);
        assertLabelMatches("Label2", "Value2", ts.getLabels()[1]);
        assertEquals(3, ts.getSamples().length);
        assertSampleMatches(0.1, 1L, ts.getSamples()[0]);
        assertSampleMatches(0.2, 2L, ts.getSamples()[1]);
        assertSampleMatches(0.3, 3L, ts.getSamples()[2]);
    }

    private static void assertLabelMatches(
            String expectedLabelName,
            String expectedLabelValue,
            PrometheusTimeSeries.Label actual) {
        assertEquals(expectedLabelName, actual.getName());
        assertEquals(expectedLabelValue, actual.getValue());
    }

    private static void assertSampleMatches(
            double expectedValue, long expectedTimestamp, PrometheusTimeSeries.Sample actual) {
        assertEquals(expectedValue, actual.getValue());
        assertEquals(expectedTimestamp, actual.getTimestamp());
    }
}
