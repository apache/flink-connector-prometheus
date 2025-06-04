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
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_LABEL_KEYS;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_NAME;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REMOTE_WRITE_URL;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_REQUEST_SIGNER;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_KEY;
import static org.apache.flink.connector.prometheus.table.PrometheusConnectorOption.METRIC_SAMPLE_TIMESTAMP;

/** Prometheus specific configuration. */
@Internal
public class PrometheusConfig implements Serializable {
    private final ReadableConfig options;

    public PrometheusConfig(ReadableConfig options) {
        this.options = options;
    }

    public String getMetricName() {
        return options.get(METRIC_NAME);
    }

    public Set<String> getLabelKeys() {
        return new HashSet<>(options.get(METRIC_LABEL_KEYS));
    }

    public String getMetricSampleKey() {
        return options.get(METRIC_SAMPLE_KEY);
    }

    public String getMetricSampleTimestamp() {
        return options.get(METRIC_SAMPLE_TIMESTAMP);
    }

    public String getRemoteWriteEndpointUrl() {
        return options.get(METRIC_REMOTE_WRITE_URL);
    }

    public String getRequestSignerIdentifier() {
        return options.get(METRIC_REQUEST_SIGNER);
    }

    public Properties toProperties() {
        Properties props = new Properties();
        props.putAll(options.toMap());
        return props;
    }
}
