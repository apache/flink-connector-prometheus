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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for querying the Prometheus HTTP API to verify that data has been successfully
 * written.
 */
public class PrometheusQueryClient implements AutoCloseable {

    private final String baseUrl;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public PrometheusQueryClient(String baseUrl) {
        this.baseUrl = baseUrl;
        this.httpClient = HttpClients.createDefault();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Query Prometheus for a metric by name and return the number of time-series results.
     *
     * @param metricName the metric name to query (PromQL instant query)
     * @return the number of time-series matching the query
     */
    public int queryMetricCount(String metricName) throws IOException, URISyntaxException {
        JsonNode result = executeQuery(metricName);
        if (result == null || !result.isArray()) {
            return 0;
        }
        return result.size();
    }

    /**
     * Query Prometheus for a metric by name and return the sample values.
     *
     * @param metricName the metric name to query
     * @return list of sample values (as doubles)
     */
    public List<Double> queryMetricValues(String metricName)
            throws IOException, URISyntaxException {
        JsonNode result = executeQuery(metricName);
        List<Double> values = new ArrayList<>();
        if (result != null && result.isArray()) {
            for (JsonNode series : result) {
                // For instant queries, "value" is [timestamp, "value"]
                JsonNode value = series.get("value");
                if (value != null && value.isArray() && value.size() >= 2) {
                    values.add(Double.parseDouble(value.get(1).asText()));
                }
            }
        }
        return values;
    }

    /**
     * Query Prometheus for a metric and verify a specific label has an expected value.
     *
     * @param metricName the metric name to query
     * @param labelName the label name to check
     * @param expectedLabelValue the expected label value
     * @return true if at least one result has the expected label value
     */
    public boolean queryMetricHasLabel(
            String metricName, String labelName, String expectedLabelValue)
            throws IOException, URISyntaxException {
        JsonNode result = executeQuery(metricName);
        if (result == null || !result.isArray()) {
            return false;
        }
        for (JsonNode series : result) {
            JsonNode metric = series.get("metric");
            if (metric != null && metric.has(labelName)) {
                if (expectedLabelValue.equals(metric.get(labelName).asText())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Execute a PromQL instant query and return the "result" array from the response.
     *
     * @param promQL the PromQL expression
     * @return the "result" JsonNode array, or null if the query failed
     */
    private JsonNode executeQuery(String promQL) throws IOException, URISyntaxException {
        URI uri = new URIBuilder(baseUrl + "/api/v1/query").addParameter("query", promQL).build();

        HttpGet request = new HttpGet(uri);
        return httpClient.execute(
                request,
                response -> {
                    String body = EntityUtils.toString(response.getEntity());
                    JsonNode root = objectMapper.readTree(body);
                    if (!"success".equals(root.path("status").asText())) {
                        return null;
                    }
                    return root.path("data").path("result");
                });
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }
}
