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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface for a request signer, specific of the Prometheus implementation.
 *
 * <p>A request signer implementation can generate additional Http request headers, based on the
 * existing headers and the request body.
 *
 * <p>The signature method is called on every write request. For performance reasons, the Map of
 * HTTP headers is mutated, instead of making a copy.
 */
@PublicEvolving
public interface PrometheusRequestSigner extends Serializable {

    /**
     * Add to the existing http request headers any additional header required by the specific
     * Prometheus implementation for signing.
     *
     * @param requestHeaders original Http request headers. For efficiency, the implementation is
     *     expected to modify the Map in place. The Map is expected to be mutable.
     * @param requestBody request body, already compressed.
     */
    void addSignatureHeaders(Map<String, String> requestHeaders, byte[] requestBody);
}
