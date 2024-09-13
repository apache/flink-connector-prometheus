/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink;

import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestEntrySizeUtilsTest {

    private static Types.Sample.Builder aSample(double value, long value1) {
        return Types.Sample.newBuilder().setValue(value).setTimestamp(value1);
    }

    private static Types.Label aLabel(String labelName, String labelValue) {
        return Types.Label.newBuilder().setName(labelName).setValue(labelValue).build();
    }

    private static Types.TimeSeries aTimeSeriesWith2Samples() {
        return Types.TimeSeries.newBuilder()
                .addSamples(aSample(0.1, 1L))
                .addSamples(aSample(0.2, 2L))
                .addLabels(aLabel("L1", "V1"))
                .build();
    }

    @Test
    void countSamples() {
        List<Types.TimeSeries> entries =
                new ArrayList<Types.TimeSeries>() {
                    {
                        add(aTimeSeriesWith2Samples());
                        add(aTimeSeriesWith2Samples());
                        add(aTimeSeriesWith2Samples());
                    }
                };

        long count = RequestEntrySizeUtils.countSamples(entries);
        assertEquals(6, count);
    }

    @Test
    void countSamplesOfEmptyList() {
        List<Types.TimeSeries> entries = Collections.emptyList();

        long count = RequestEntrySizeUtils.countSamples(entries);
        assertEquals(0, count);
    }

    @Test
    void requestSizeForBatching() {
        Types.TimeSeries ts = aTimeSeriesWith2Samples();

        long sampleCount = RequestEntrySizeUtils.requestSizeForBatching(ts);
        assertEquals(2, sampleCount);
    }

    @Test
    void requestSerializedSize() {
        Types.TimeSeries ts = aTimeSeriesWith2Samples();
        long serializedSize = RequestEntrySizeUtils.requestSerializedSize(ts);
        int protobufSerializedSize = ts.getSerializedSize();

        assertEquals(protobufSerializedSize, (int) serializedSize);
    }
}
