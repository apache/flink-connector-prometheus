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

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrometheusStateSerializerTest {

    private static final ElementConverter<PrometheusTimeSeries, Types.TimeSeries>
            ELEMENT_CONVERTER = new PrometheusTimeSeriesConverter();

    private static PrometheusTimeSeries getTestTimeSeries(int i) {
        return PrometheusTimeSeries.builder()
                .withMetricName("metric-name")
                .addLabel("dimensionA", "value-" + i)
                .addSample(i + 42.0, i + 1L)
                .addSample(i + 3.14, i + 2L)
                .build();
    }

    /**
     * This method uses the same implementation as PrometheusSinkWriter.getSizeInBytes() to extract
     * the requestEntry "size" (i.e. the number of Samples). This is the "size" used in
     * RequestEntryWrapper.
     *
     * <p>See
     * https://github.com/apache/flink/blob/69e812688b43be9a0c4f79e6af81bc2d1d8a873e/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/sink/writer/AsyncSinkWriterStateSerializer.java#L60
     */
    private static int getRequestSize(Types.TimeSeries requestEntry) {
        return requestEntry.getSamplesCount();
    }

    private static BufferedRequestState<Types.TimeSeries> getTestState() {
        return new BufferedRequestState<>(
                IntStream.range(0, 10)
                        .mapToObj(PrometheusStateSerializerTest::getTestTimeSeries)
                        .map((element) -> ELEMENT_CONVERTER.apply(element, null))
                        .map(
                                (requestEntry) ->
                                        new RequestEntryWrapper<>(
                                                requestEntry, getRequestSize(requestEntry)))
                        .collect(Collectors.toList()));
    }

    private void assertThatBufferStatesAreEqual(
            BufferedRequestState<Types.TimeSeries> actualBuffer,
            BufferedRequestState<Types.TimeSeries> expectedBuffer) {
        Assertions.assertThat(actualBuffer.getStateSize()).isEqualTo(expectedBuffer.getStateSize());
        int actualLength = actualBuffer.getBufferedRequestEntries().size();
        Assertions.assertThat(actualLength)
                .isEqualTo(expectedBuffer.getBufferedRequestEntries().size());
        List<RequestEntryWrapper<Types.TimeSeries>> actualRequestEntries =
                actualBuffer.getBufferedRequestEntries();
        List<RequestEntryWrapper<Types.TimeSeries>> expectedRequestEntries =
                expectedBuffer.getBufferedRequestEntries();

        for (int i = 0; i < actualLength; i++) {
            // Protobuf-generated objects like Types.TimeSeries implements equals() with all nested
            // objects
            Assertions.assertThat(actualRequestEntries.get(i).getRequestEntry())
                    .isEqualTo(expectedRequestEntries.get(i).getRequestEntry());
            Assertions.assertThat(actualRequestEntries.get(i).getSize())
                    .isEqualTo(expectedRequestEntries.get(i).getSize());
        }
    }

    @Test
    void testSerializeDeserialize() throws IOException {
        BufferedRequestState<Types.TimeSeries> expectedState = getTestState();
        PrometheusStateSerializer serializer = new PrometheusStateSerializer();

        byte[] serializedExpectedState = serializer.serialize(expectedState);
        BufferedRequestState<Types.TimeSeries> actualState =
                serializer.deserialize(1, serializedExpectedState);
        assertThatBufferStatesAreEqual(actualState, expectedState);
    }

    @Test
    public void testVersion() {
        PrometheusStateSerializer serializer = new PrometheusStateSerializer();
        assertEquals(1, serializer.getVersion());
    }
}
