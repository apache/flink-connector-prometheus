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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PrometheusTimeSeriesLabelsAndMetricNameKeySelectorTest {

    private PrometheusTimeSeriesLabelsAndMetricNameKeySelector selector = new PrometheusTimeSeriesLabelsAndMetricNameKeySelector();


    @Test
    void timeSeriesWithSameLabelsAndMetricNameShouldHaveSameKey() throws Exception {
        PrometheusTimeSeries ts1 = PrometheusTimeSeries.builder()
                .withMetricName("metric1")
                .addLabel("label1", "value1")
                .addLabel("label2", "value2")
                .addSample(42, 1L)
                .addSample(3.14, 2L)
                .build();

        PrometheusTimeSeries ts2 = PrometheusTimeSeries.builder()
                .withMetricName("metric1")
                .addLabel("label1", "value1")
                .addLabel("label2", "value2")
                .addSample(57, 1L)
                .addSample(123, 2L)
                .build();

        assertEquals(selector.getKey(ts1), selector.getKey(ts2));
    }


    @Test
    void timeSeriesWithDifferentLabelValuesAndSameMetricNameShouldHaveDifferentKey() throws Exception {
        PrometheusTimeSeries ts1 = PrometheusTimeSeries.builder()
                .withMetricName("metric1")
                .addLabel("label1", "valueX")
                .addLabel("label2", "valueY")
                .addSample(42, 1L)
                .addSample(3.14, 2L)
                .build();

        PrometheusTimeSeries ts2 = PrometheusTimeSeries.builder()
                .withMetricName("metric1")
                .addLabel("label1", "value1")
                .addLabel("label2", "value2")
                .addSample(42, 1L)
                .addSample(3.14, 2L)
                .build();

        assertNotEquals(selector.getKey(ts1), selector.getKey(ts2));
    }

    @Test
    void timeSeriesWithSameLabelsAndDifferentMetricNameShouldHaveDifferentKey() throws Exception {
        PrometheusTimeSeries ts1 = PrometheusTimeSeries.builder()
                .withMetricName("metric1")
                .addLabel("label1", "value1")
                .addLabel("label2", "value2")
                .addSample(42, 1L)
                .addSample(3.14, 2L)
                .build();

        PrometheusTimeSeries ts2 = PrometheusTimeSeries.builder()
                .withMetricName("metric2")
                .addLabel("label1", "value1")
                .addLabel("label2", "value2")
                .addSample(42, 1L)
                .addSample(3.14, 2L)
                .build();

        assertNotEquals(selector.getKey(ts1), selector.getKey(ts2));
    }


}