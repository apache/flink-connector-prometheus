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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrometheusTimeSeriesConverterTest {

    private PrometheusTimeSeriesConverter sut = new PrometheusTimeSeriesConverter();

    private SinkWriter.Context dummyContext =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return 0L;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    @Test
    public void testMetricNameLabel() {
        PrometheusTimeSeries input =
                PrometheusTimeSeries.builder()
                        .withMetricName("metric-1")
                        .addSample(42.0, 1L)
                        .build();

        Types.TimeSeries requestEntry = sut.apply(input, dummyContext);

        assertEquals(1, requestEntry.getLabelsList().size());

        Types.Label firstLabel = requestEntry.getLabelsList().get(0);
        assertEquals("__name__", firstLabel.getName());
        assertEquals("metric-1", firstLabel.getValue());
    }

    @Test
    public void testAdditionalLabels() {
        PrometheusTimeSeries input =
                PrometheusTimeSeries.builder()
                        .withMetricName("metric-1")
                        .addLabel("dimensionA", "value-A")
                        .addLabel("dimensionB", "value-B")
                        .addSample(42, 1L)
                        .build();

        Types.TimeSeries requestEntry = sut.apply(input, dummyContext);

        assertEquals(3, requestEntry.getLabelsList().size());

        Types.Label secondLabel = requestEntry.getLabelsList().get(1);
        assertEquals("dimensionA", secondLabel.getName());
        assertEquals("value-A", secondLabel.getValue());

        Types.Label thirdLabel = requestEntry.getLabelsList().get(2);
        assertEquals("dimensionB", thirdLabel.getName());
        assertEquals("value-B", thirdLabel.getValue());
    }

    @Test
    public void testSamples() {
        PrometheusTimeSeries input =
                PrometheusTimeSeries.builder()
                        .withMetricName("metric-1")
                        .addSample(42.0, 1L)
                        .addSample(3.14, 2L)
                        .build();

        Types.TimeSeries requestEntry = sut.apply(input, dummyContext);

        assertEquals(2, requestEntry.getSamplesList().size());

        Types.Sample firstSample = requestEntry.getSamplesList().get(0);
        assertEquals(42.0d, firstSample.getValue());
        assertEquals(1L, firstSample.getTimestamp());

        Types.Sample secondSample = requestEntry.getSamplesList().get(1);
        assertEquals(3.14d, secondSample.getValue());
        assertEquals(2L, secondSample.getTimestamp());
    }
}
