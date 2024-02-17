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
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

/**
 * Converts the sink input {@link PrometheusTimeSeries} into the Protobuf {@link Types.TimeSeries}
 * that are sent to Prometheus.
 */
public class PrometheusTimeSeriesConverter
        implements ElementConverter<PrometheusTimeSeries, Types.TimeSeries> {

    private static final String METRIC_NAME_LABEL_NAME = "__name__";

    @Override
    public Types.TimeSeries apply(PrometheusTimeSeries element, SinkWriter.Context context) {
        var builder =
                Types.TimeSeries.newBuilder()
                        .addLabels(
                                Types.Label.newBuilder()
                                        .setName(METRIC_NAME_LABEL_NAME)
                                        .setValue(element.getMetricName())
                                        .build());

        for (PrometheusTimeSeries.Label label : element.getLabels()) {
            builder.addLabels(
                    Types.Label.newBuilder()
                            .setName(label.getName())
                            .setValue(label.getValue())
                            .build());
        }

        for (PrometheusTimeSeries.Sample sample : element.getSamples()) {
            builder.addSamples(
                    Types.Sample.newBuilder()
                            .setValue(sample.getValue())
                            .setTimestamp(sample.getTimestamp())
                            .build());
        }

        return builder.build();
    }
}
