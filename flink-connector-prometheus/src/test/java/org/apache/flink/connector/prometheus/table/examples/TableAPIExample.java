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

package org.apache.flink.connector.prometheus.table.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.row;

/**
 * Example application demonstrating the usage of the Prometheus sink connector.
 *
 * <p>The application expects a single configuration parameter, with the RemoteWrite endpoint URL:
 * --prometheusRemoteWriteUrl &lt;URL&gt;
 *
 * <p>The application generates rowData internally and sinks to Prometheus.
 */
public class TableAPIExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableAPIExample.class);

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        ParameterTool applicationParameters = ParameterTool.fromArgs(args);

        // Prometheus remote-write URL
        String prometheusRemoteWriteUrl = applicationParameters.get("prometheusRemoteWriteUrl");
        LOGGER.info("Prometheus URL:{}", prometheusRemoteWriteUrl);

        tEnv.createTable(
                "PrometheusSinkTable",
                TableDescriptor.forConnector("prometheus")
                        .schema(
                                Schema.newBuilder()
                                        .column("test_metric_name", DataTypes.STRING())
                                        .column("test_label_name", DataTypes.STRING())
                                        .column("test_sample_key", DataTypes.DOUBLE())
                                        .column("test_sample_ts_key", DataTypes.TIMESTAMP())
                                        .build())
                        .option("metric.name", "test_metric_name")
                        .option("metric.label.keys", "test_label_name")
                        .option("metric.sample.key", "test_sample_key")
                        .option("metric.sample.timestamp", "test_sample_ts_key")
                        .option("sink.batch.max-size", "2")
                        .option("metric.endpoint-url", prometheusRemoteWriteUrl)
                        // Uncomment the following line to enable the
                        // AmazonManagedPrometheusWriteRequestSigner
                        // for signing requests when writing to Amazon Managed Prometheus
                        // .option("metric.request-signer", "amazon-managed-prometheus")
                        // .option("aws.region", "us-east-1")
                        // .option("aws.credentials.provider", "BASIC")
                        // .option("aws.credentials.provider.basic.accesskeyid", "accesskey")
                        // .option("aws.credentials.provider.basic.secretkey", "secretkey")
                        .build());

        tEnv.fromValues(
                        row("test_metric_name_1", "label_1", 1.0d, Instant.now()),
                        row("test_metric_name_1", "label_2", 2.0d, Instant.now()),
                        row("test_metric_name_1", "label_3", 3.0d, Instant.now()))
                .insertInto("PrometheusSinkTable")
                .execute();
    }
}
