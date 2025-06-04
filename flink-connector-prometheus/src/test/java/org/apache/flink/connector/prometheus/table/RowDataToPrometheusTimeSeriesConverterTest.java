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

import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_LABEL_KEYS;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_NAME;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REMOTE_WRITE_URL;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_KEY;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_TIMESTAMP;
import static org.apache.flink.connector.prometheus.table.TestUtils.DATA_TYPE;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_LABEL_KEY;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_LABEL_VALUE;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_METRIC_NAME;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_REMOTE_WRITE_ENDPOINT;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_SAMPLE_KEY;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_SAMPLE_TS_KEY;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_SAMPLE_TS_VALUE;
import static org.apache.flink.connector.prometheus.table.TestUtils.TEST_SAMPLE_VALUE;
import static org.apache.flink.connector.prometheus.table.TestUtils.createElement;
import static org.assertj.core.api.Assertions.assertThat;

class RowDataToPrometheusTimeSeriesConverterTest {
    private PrometheusConfig prometheusConfig;
    private RowDataToPrometheusTimeSeriesConverter rowDataToPrometheusTimeSeriesConverter;

    @BeforeEach
    void setUp() {
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.set(METRIC_NAME, TEST_METRIC_NAME);
        tableConfig.set(METRIC_LABEL_KEYS, Collections.singletonList(TEST_LABEL_KEY));
        tableConfig.set(METRIC_SAMPLE_KEY, TEST_SAMPLE_KEY);
        tableConfig.set(METRIC_SAMPLE_TIMESTAMP, TEST_SAMPLE_TS_KEY);
        tableConfig.set(METRIC_REMOTE_WRITE_URL, TEST_REMOTE_WRITE_ENDPOINT);

        prometheusConfig = new PrometheusConfig(tableConfig);

        rowDataToPrometheusTimeSeriesConverter =
                new RowDataToPrometheusTimeSeriesConverter(DATA_TYPE, prometheusConfig);
    }

    @Test
    void convertRowData() {
        RowData row = createElement();
        PrometheusTimeSeries actualPrometheusTimeSeries =
                rowDataToPrometheusTimeSeriesConverter.convertRowData(row);

        PrometheusTimeSeries expectedPrometheusTimeSeries =
                PrometheusTimeSeries.builder()
                        .withMetricName(TEST_METRIC_NAME)
                        .addLabel(TEST_LABEL_KEY, TEST_LABEL_VALUE)
                        .addSample(TEST_SAMPLE_VALUE, TEST_SAMPLE_TS_VALUE)
                        .build();

        assertThat(actualPrometheusTimeSeries)
                .usingRecursiveComparison()
                .isEqualTo(expectedPrometheusTimeSeries);
    }
}
