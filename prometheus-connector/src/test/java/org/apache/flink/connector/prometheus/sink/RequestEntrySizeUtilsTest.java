package org.apache.flink.connector.prometheus.sink;

import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.junit.jupiter.api.Test;

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
                List.of(
                        aTimeSeriesWith2Samples(),
                        aTimeSeriesWith2Samples(),
                        aTimeSeriesWith2Samples());

        long count = RequestEntrySizeUtils.countSamples(entries);
        assertEquals(6, count);
    }

    @Test
    void countSamplesOfEmptyList() {
        List<Types.TimeSeries> entries = List.of();

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
