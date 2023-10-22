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

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Serializes/deserializes the sink request-entry, the protobuf-generated {@link Types.TimeSeries}, using protobuf.
 */
public class PrometheusStateSerializer extends AsyncSinkWriterStateSerializer<Types.TimeSeries> {
    private static final int VERSION = 1;


    // Copied from AsyncSinkWriterStateSerializer.DATA_IDENTIFIER
    private static final long DATA_IDENTIFIER = -1;

    @Override
    protected void serializeRequestToStream(Types.TimeSeries request, DataOutputStream out) throws IOException {
        byte[] serializedRequest = request.toByteArray();
        out.write(serializedRequest);
    }

    @Override
    protected Types.TimeSeries deserializeRequestFromStream(long requestSize, DataInputStream in) throws IOException {
        // The size written into the serialized stat is the size of the protobuf-serialized time-series
        byte[] requestData = new byte[(int) requestSize];
        in.read(requestData);
        return Types.TimeSeries.parseFrom(requestData);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    /**
     * Overrides the original implementation that assumes the serialized size is the value returned by
     * {@link PrometheusSinkWriter#getSizeInBytes(Types.TimeSeries)}
     * <p>
     * Most of the code is copied from the original implementation of AsyncSinkWriterStateSerializer.serialize().
     * <p>
     * The state is serialized in form of [DATA_IDENTIFIER,NUM_OF_ELEMENTS,SIZE1,REQUEST1,SIZE2,REQUEST2....], where
     * REQUESTn is the Protobuf-serialized representation of a {@link Types.TimeSeries TimeSeries}.
     * In this implementation SIZEn is the size of the Protobuf serialization, in bytes, that does not match the "size"
     * of a {@link RequestEntryWrapper}.
     *
     * @param bufferedRequestState The buffered request state to be serialized
     * @return serialized buffered request state
     * @throws IOException
     */
    @Override
    public byte[] serialize(BufferedRequestState<Types.TimeSeries> bufferedRequestState) throws IOException {
        Collection<RequestEntryWrapper<Types.TimeSeries>> bufferState = bufferedRequestState.getBufferedRequestEntries();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {

            out.writeLong(DATA_IDENTIFIER); // DATA_IDENTIFIER
            out.writeInt(bufferState.size()); // NUM_OF_ELEMENTS

            for (RequestEntryWrapper<Types.TimeSeries> wrapper : bufferState) {
                // In the serialized state we write the size of the serialized representation, rather than the size
                // held in RequestEntryWrapper, that is the output of AsyncSinkWriter.getSizeInBytes()
                long requestEntrySize = RequestEntrySizeUtils.requestSerializedSize(wrapper.getRequestEntry());
                out.writeLong(requestEntrySize); // SIZEn
                serializeRequestToStream(wrapper.getRequestEntry(), out); // REQUESTn
            }

            return baos.toByteArray();
        }
    }

    /**
     * Overrides the original implementation that assumes the serialized size is the value returned by
     * {@link PrometheusSinkWriter#getSizeInBytes(Types.TimeSeries)}
     * <p>
     * See {@link PrometheusStateSerializer#serialize(BufferedRequestState)} for more details.
     *
     * @param version    The version in which the data was serialized
     * @param serialized The serialized data
     * @return a buffered request state wrapping the deserialized time-series.
     * @throws IOException
     */
    @Override
    public BufferedRequestState<Types.TimeSeries> deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {

            validateIdentifier(in); // DATA_IDENTIFIER
            int numberOfElements = in.readInt(); // NUM_OF_ELEMENTS

            List<RequestEntryWrapper<Types.TimeSeries>> serializedState = new ArrayList<>();
            for (int i = 0; i < numberOfElements; i++) {
                // This is the size of an request-entry in the serialized state
                long requestSerializedSize = in.readLong(); // SIZEn - the serialized size
                Types.TimeSeries requestEntry = deserializeRequestFromStream(requestSerializedSize, in);

                // The "size" of RequestEntryWrapper must be the size returned by PrometheusSinkWriter.getSizeInBytes(),
                // used for batching
                long requestEntrySize = RequestEntrySizeUtils.requestSizeForBatching(requestEntry);
                serializedState.add(new RequestEntryWrapper<>(requestEntry, requestEntrySize));
            }

            return new BufferedRequestState<>(serializedState);
        }
    }

    // Copy of the private implementation of AsyncSinkWriterStateSerializer.validateIdentifier()
    private void validateIdentifier(DataInputStream in) throws IOException {
        if (in.readLong() != DATA_IDENTIFIER) {
            throw new IllegalStateException("Corrupted data to deserialize");
        }
    }
}
