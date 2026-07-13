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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A Testcontainers-based Prometheus container configured with Remote Write Receiver enabled.
 *
 * <p>This container starts Prometheus with the {@code --web.enable-remote-write-receiver} flag,
 * which enables the /api/v1/write endpoint for receiving remote write requests.
 *
 * <p>If Testcontainers cannot detect Docker automatically (e.g., Docker Desktop on WSL2), set the
 * {@code DOCKER_HOST} environment variable or create {@code ~/.testcontainers.properties} with
 * {@code docker.host=unix:///var/run/docker.sock}.
 */
public class PrometheusContainer extends GenericContainer<PrometheusContainer> {

    private static final DockerImageName DEFAULT_IMAGE =
            DockerImageName.parse("prom/prometheus:v2.51.0");
    private static final int PROMETHEUS_PORT = 9090;

    public PrometheusContainer() {
        this(DEFAULT_IMAGE);
    }

    public PrometheusContainer(DockerImageName imageName) {
        super(imageName);
        withExposedPorts(PROMETHEUS_PORT);
        // Enable the Remote Write Receiver so we can POST time-series data
        withCommand(
                "--config.file=/etc/prometheus/prometheus.yml",
                "--web.enable-remote-write-receiver",
                "--storage.tsdb.retention.time=1h");
        waitingFor(Wait.forHttp("/-/ready").forPort(PROMETHEUS_PORT).forStatusCode(200));
    }

    /** Returns the full URL for the Remote Write endpoint. */
    public String getRemoteWriteUrl() {
        return String.format(
                "http://%s:%d/api/v1/write", getHost(), getMappedPort(PROMETHEUS_PORT));
    }

    /** Returns the full URL for the Prometheus HTTP query API. */
    public String getQueryApiUrl() {
        return String.format(
                "http://%s:%d/api/v1/query", getHost(), getMappedPort(PROMETHEUS_PORT));
    }

    /** Returns the base URL of the Prometheus HTTP API. */
    public String getBaseUrl() {
        return String.format("http://%s:%d", getHost(), getMappedPort(PROMETHEUS_PORT));
    }
}
