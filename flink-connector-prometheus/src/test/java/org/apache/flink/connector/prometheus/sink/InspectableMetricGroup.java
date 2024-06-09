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

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Test implementation of MetricGroup that allows inspecting the value of a metrics. The current
 * implementation only supports Counters.
 */
public class InspectableMetricGroup implements MetricGroup {

    private final Map<String, Counter> counters = new HashMap<>();

    @Override
    public Counter counter(String name) {
        Counter counter = new SimpleCounter();
        counters.put(name, counter);
        return counter;
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        counters.put(name, counter);
        return counter;
    }

    public long getCounterCount(String name) {
        if (counters.containsKey(name)) {
            return counters.get(name).getCount();
        } else {
            return 0L;
        }
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        throw new UnsupportedOperationException("Gauges are not supported by this metric group");
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
        throw new UnsupportedOperationException("Histograms not supported by this metric group");
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        throw new UnsupportedOperationException("Meters not supported by this metric group");
    }

    @Override
    public MetricGroup addGroup(String s) {
        return new InspectableMetricGroup();
    }

    @Override
    public MetricGroup addGroup(String s, String s1) {
        return new InspectableMetricGroup();
    }

    @Override
    public String[] getScopeComponents() {
        return new String[0];
    }

    @Override
    public Map<String, String> getAllVariables() {
        return Collections.emptyMap();
    }

    @Override
    public String getMetricIdentifier(String metricName) {
        return metricName;
    }

    @Override
    public String getMetricIdentifier(String metricName, CharacterFilter filter) {
        return metricName;
    }
}
