/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.prometheus.sink.errorhandling.SinkWriterErrorHandlingBehaviorConfiguration;
import org.apache.flink.connector.prometheus.sink.http.PrometheusAsyncHttpClientBuilder;
import org.apache.flink.connector.prometheus.sink.http.RetryConfiguration;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.apache.hc.core5.http.HttpHeaders;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class PrometheusSinkWriterTest {

    private static final int MAX_IN_FLIGHT_REQUESTS = 1;
    private static final int MAX_BUFFERED_REQUESTS = 512;
    private static final int MAX_BATCH_SIZE_IN_SAMPLES = 97;
    private static final int MAX_RECORD_SIZE_IN_SAMPLES = 31;
    private static final long MAX_TIME_IN_BUFFER_MS = 513;
    private static final String PROMETHEUS_REMOTE_WRITE_URL = "https://foo.bar/baz";

    private static final ElementConverter<PrometheusTimeSeries, Types.TimeSeries>
            ELEMENT_CONVERTER = new PrometheusTimeSeriesConverter();
    private static final PrometheusAsyncHttpClientBuilder CLIENT_BUILDER =
            new PrometheusAsyncHttpClientBuilder(RetryConfiguration.builder().build());
    private static final PrometheusRequestSigner REQUEST_SIGNER =
            new DummyPrometheusRequestSigner();
    private static final String HTTP_USER_AGENT = "Test-User-Agent";
    private static final SinkWriterErrorHandlingBehaviorConfiguration
            ERROR_HANDLING_BEHAVIOR_CONFIGURATION =
                    SinkWriterErrorHandlingBehaviorConfiguration.DEFAULT_BEHAVIORS;
    private static final String METRIC_GROUP_NAME = "test-group";

    @Test
    void testInizializeAsyncSinkBaseParameters() throws Exception {
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();

        PrometheusSink sink =
                new PrometheusSink(
                        ELEMENT_CONVERTER,
                        MAX_IN_FLIGHT_REQUESTS,
                        MAX_BUFFERED_REQUESTS,
                        MAX_BATCH_SIZE_IN_SAMPLES,
                        MAX_RECORD_SIZE_IN_SAMPLES,
                        MAX_TIME_IN_BUFFER_MS,
                        PROMETHEUS_REMOTE_WRITE_URL,
                        CLIENT_BUILDER,
                        REQUEST_SIGNER,
                        HTTP_USER_AGENT,
                        ERROR_HANDLING_BEHAVIOR_CONFIGURATION,
                        METRIC_GROUP_NAME);

        PrometheusSinkWriter sinkWriter = (PrometheusSinkWriter) sink.createWriter(sinkInitContext);

        assertThat(sinkWriter).extracting("maxBatchSize").isEqualTo(MAX_BATCH_SIZE_IN_SAMPLES);
        assertThat(sinkWriter)
                .extracting("maxBatchSizeInBytes")
                .isEqualTo((long) MAX_BATCH_SIZE_IN_SAMPLES);

        assertThat(sinkWriter).extracting("maxBufferedRequests").isEqualTo(MAX_BUFFERED_REQUESTS);
        assertThat(sinkWriter)
                .extracting("maxRecordSizeInBytes")
                .isEqualTo((long) MAX_RECORD_SIZE_IN_SAMPLES);
        assertThat(sinkWriter).extracting("maxTimeInBufferMS").isEqualTo(MAX_TIME_IN_BUFFER_MS);

        assertThat(sinkWriter)
                .extracting("requestBuilder")
                .extracting("prometheusRemoteWriteUrl")
                .isEqualTo(PROMETHEUS_REMOTE_WRITE_URL);
        assertThat(sinkWriter)
                .extracting("requestBuilder")
                .extracting("requestSigner")
                .isEqualTo(REQUEST_SIGNER);
        assertThat(sinkWriter)
                .extracting("requestBuilder")
                .extracting("fixedHeaders")
                .extracting(HttpHeaders.USER_AGENT)
                .isEqualTo(HTTP_USER_AGENT);

        assertThat(sinkWriter)
                .extracting("errorHandlingBehaviorConfig")
                .isEqualTo(ERROR_HANDLING_BEHAVIOR_CONFIGURATION);
    }
}
