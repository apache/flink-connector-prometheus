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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class TestUtils {
    static final String TEST_METRIC_NAME = "test_metric_name";
    static final String TEST_LABEL_KEY = "test_label_key";
    static final String TEST_LABEL_VALUE = "test_label_val";
    static final String TEST_SAMPLE_KEY = "test_sample_key";
    static final double TEST_SAMPLE_VALUE = 123d;
    static final String TEST_SAMPLE_TS_KEY = "test_sample_ts_key";
    static final long TEST_SAMPLE_TS_VALUE = 234L;
    static final String TEST_REMOTE_WRITE_ENDPOINT = "https://test_endpoint_url";
    static final String TEST_REQUEST_SIGNER = "test_signer";
    static final String TEST_ENDPOINT_REGION = "test_region";

    static final DataType DATA_TYPE =
            DataTypes.ROW(
                            DataTypes.FIELD(TEST_METRIC_NAME, DataTypes.STRING()),
                            DataTypes.FIELD(TEST_LABEL_KEY, DataTypes.STRING()),
                            DataTypes.FIELD(TEST_SAMPLE_KEY, DataTypes.DOUBLE()),
                            DataTypes.FIELD(TEST_SAMPLE_TS_KEY, DataTypes.TIMESTAMP()))
                    .notNull();

    static RowData createElement() {
        GenericRowData element = new GenericRowData(RowKind.INSERT, 4);
        element.setField(0, StringData.fromString(TEST_METRIC_NAME));
        element.setField(1, StringData.fromString(TEST_LABEL_VALUE));
        element.setField(2, TEST_SAMPLE_VALUE);
        element.setField(3, TimestampData.fromEpochMillis(TEST_SAMPLE_TS_VALUE));
        return element;
    }
}
