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

package org.apache.flink.connector.prometheus.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * Converts the sink input {@link PrometheusTimeSeries} into the Protobuf {@link Types.TimeSeries}
 * that are sent to Prometheus.
 */
@PublicEvolving
public class RowDataElementConverter implements ElementConverter<RowData, Types.TimeSeries> {
    private final DataType physicalDataType;
    private final PrometheusConfig prometheusConfig;
    private transient RowDataToPrometheusTimeSeriesConverter rowDataToPrometheusTimeSeriesConverter;

    public RowDataElementConverter(DataType physicalDataType, PrometheusConfig prometheusConfig) {
        this.physicalDataType = physicalDataType;
        this.prometheusConfig = prometheusConfig;
        this.rowDataToPrometheusTimeSeriesConverter =
                new RowDataToPrometheusTimeSeriesConverter(physicalDataType, prometheusConfig);
    }

    public Types.TimeSeries apply(RowData element, SinkWriter.Context context) {
        if (rowDataToPrometheusTimeSeriesConverter == null) {
            rowDataToPrometheusTimeSeriesConverter =
                    new RowDataToPrometheusTimeSeriesConverter(physicalDataType, prometheusConfig);
        }

        return rowDataToPrometheusTimeSeriesConverter.convertRowData(element).toTimeSeries();
    }
}
