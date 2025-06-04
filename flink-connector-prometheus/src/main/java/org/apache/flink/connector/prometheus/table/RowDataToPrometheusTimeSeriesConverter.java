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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import java.util.List;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/**
 * Converts from Flink Table API internal type of {@link RowData} to {@link PrometheusTimeSeries}.
 */
@Internal
public class RowDataToPrometheusTimeSeriesConverter {

    private final DataType physicalDataType;
    private final PrometheusConfig prometheusConfig;

    public RowDataToPrometheusTimeSeriesConverter(
            DataType physicalDataType, PrometheusConfig prometheusConfig) {
        this.physicalDataType = physicalDataType;
        this.prometheusConfig = prometheusConfig;
    }

    public PrometheusTimeSeries convertRowData(RowData row) {
        List<DataTypes.Field> fields = DataType.getFields(physicalDataType);

        PrometheusTimeSeries.Builder builder = PrometheusTimeSeries.builder();
        Double sampleValue = null;
        Long sampleTimestamp = null;

        for (int i = 0; i < fields.size(); i++) {
            DataTypes.Field field = fields.get(i);
            RowData.FieldGetter fieldGetter =
                    createFieldGetter(fields.get(i).getDataType().getLogicalType(), i);
            FieldValue fieldValue = new FieldValue(fieldGetter.getFieldOrNull(row));
            String fieldName = field.getName();

            if (fieldName.equals(prometheusConfig.getMetricName())) {
                builder.withMetricName(fieldValue.getStringValue());
            } else if (fieldName.equals(prometheusConfig.getMetricSampleKey())) {
                sampleValue = fieldValue.getDoubleValue();
            } else if (prometheusConfig.getLabelKeys().contains(fieldName)) {
                builder.addLabel(fieldName, fieldValue.getStringValue());
            } else if (fieldName.equals(prometheusConfig.getMetricSampleTimestamp())) {
                sampleTimestamp = fieldValue.getLongValue();
            }
        }

        if (sampleValue != null && sampleTimestamp != null) {
            builder.addSample(sampleValue, sampleTimestamp);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Row is missing sampleValue field; %s or sampleTimestamp: %s",
                            sampleValue, sampleTimestamp));
        }

        return builder.build();
    }

    private static class FieldValue {
        private final Object value;

        private FieldValue(Object value) {
            this.value = value;
        }

        private String getStringValue() {
            if (value instanceof StringData) {
                return value.toString();
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Field: %s of type: %s is not a valid StringData type",
                                value.toString(), value.getClass()));
            }
        }

        private Double getDoubleValue() {
            return Double.valueOf(value.toString());
        }

        private Long getLongValue() {
            if (value instanceof TimestampData) {
                return ((TimestampData) value).getMillisecond();
            }
            return Long.valueOf(value.toString());
        }
    }
}
