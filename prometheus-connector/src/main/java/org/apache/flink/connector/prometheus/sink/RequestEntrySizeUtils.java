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

import java.util.Collection;

/**
 * Collection of methods to calculate the sink RequestEntry "size"
 */
public class RequestEntrySizeUtils {

    /**
     * Size of a  request entry (a {@link Types.TimeSeries time-series}) for the purpose of batching.
     * Count the number of {@link Types.Sample samples}
     *
     * @param requestEntry a time-series
     * @return number of Samples in the TimeSeries
     */
    public static long requestSizeForBatching(Types.TimeSeries requestEntry) {
        return requestEntry.getSamplesCount();
    }

    /**
     * Serialized size of a request entry {@link Types.TimeSeries TimeSeries}: the number of bytes of the
     * protobuf- serialized representation of the TimeSeries.
     *
     * @param requestEntry a time-series
     * @return number of bytes
     */
    public static long requestSerializedSize(Types.TimeSeries requestEntry) {
        return requestEntry.getSerializedSize();
    }

    /**
     * Count the number of {@link Types.Sample samples} in a collection of {@link Types.TimeSeries time-series}
     * (a batch).
     *
     * @param requestEntries collection of time-series
     * @return number of samples
     */
    public static long countSamples(Collection<Types.TimeSeries> requestEntries) {
        return requestEntries.stream().mapToLong(RequestEntrySizeUtils::requestSizeForBatching).sum();
    }
}
