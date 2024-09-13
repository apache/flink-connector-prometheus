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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Implementation of KeySelector ensuring the {@link PrometheusTimeSeries} with the same set of
 * labels end up in the same partition.
 *
 * <p>For using the sink with parallelism > 1, the input of the sink must be a keyedStream using
 * this KeySelector to extract the key. This guarantees TimeSeries with the same set of labels are
 * written to Prometheus in the same order they are sent to the sink.
 *
 * <p>The partition key is the hash of all labels AND the metricName. The metricName is added as
 * additional label by the sink, before writing to Prometheus.
 */
@PublicEvolving
public class PrometheusTimeSeriesLabelsAndMetricNameKeySelector
        implements KeySelector<PrometheusTimeSeries, Integer> {
    @Override
    public Integer getKey(PrometheusTimeSeries timeSeries) throws Exception {
        return new HashCodeBuilder(17, 37)
                .append(timeSeries.getLabels())
                .append(timeSeries.getMetricName())
                .build();
    }
}
