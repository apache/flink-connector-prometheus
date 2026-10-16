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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

public class PrometheusConnectorOption {

    // -----------------------------------------------------------------------------------------
    // Prometheus connector specific options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<String> METRIC_NAME =
            ConfigOptions.key("metric.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prometheus metric name.");

    public static final ConfigOption<List<String>> METRIC_LABEL_KEYS =
            ConfigOptions.key("metric.label.keys")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Prometheus metric label key name list.");

    public static final ConfigOption<String> METRIC_SAMPLE_KEY =
            ConfigOptions.key("metric.sample.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prometheus metric sample key.");

    public static final ConfigOption<String> METRIC_SAMPLE_TIMESTAMP =
            ConfigOptions.key("metric.sample.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prometheus metric sample timestamp.");

    public static final ConfigOption<String> METRIC_REMOTE_WRITE_URL =
            ConfigOptions.key("metric.endpoint-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prometheus remote write URL.");

    public static final ConfigOption<String> METRIC_REQUEST_SIGNER =
            ConfigOptions.key("metric.request-signer")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prometheus metric request signer.");
}
