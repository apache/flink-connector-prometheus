/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink.aws;

import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.connector.prometheus.table.PrometheusConfig;
import org.apache.flink.connector.prometheus.table.PrometheusDynamicRequestSignerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Properties;

public class AmazonManagedPrometheusWriteRequestSignerFactory
        implements PrometheusDynamicRequestSignerFactory {
    public AmazonManagedPrometheusWriteRequestSignerFactory() {}

    @Override
    public String requestSignerIdentifer() {
        return "amazon-managed-prometheus";
    }

    @Override
    public PrometheusRequestSigner getRequestSigner(PrometheusConfig config) {
        Properties properties = config.toProperties();
        AWSGeneralUtil.validateAwsConfiguration(properties);

        final AwsCredentialsProvider credentialsProvider =
                AWSGeneralUtil.getCredentialsProvider(properties);
        final String awsRegion = AWSGeneralUtil.getRegion(properties).toString();
        final String remoteWriteUrl = config.getRemoteWriteEndpointUrl();

        return new AmazonManagedPrometheusWriteRequestSigner(
                remoteWriteUrl, awsRegion, credentialsProvider);
    }
}
