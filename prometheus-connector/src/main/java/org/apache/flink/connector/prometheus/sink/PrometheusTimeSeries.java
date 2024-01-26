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

import org.apache.flink.annotation.Public;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Pojo used as sink input, containing a single TimeSeries: a list of Labels and a list of Samples.
 *
 * <p>metricName is mapped in Prometheus to the value of the mandatory label named '__name__'
 * labels. The other labels, as key/value, are appended after the '__name__' label.
 */
@Public
public class PrometheusTimeSeries implements Serializable {
    /** A single Label. */
    public static class Label implements Serializable {
        private final String name;
        private final String value;

        public Label(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Label label = (Label) o;
            return new EqualsBuilder()
                    .append(name, label.name)
                    .append(value, label.value)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(name).append(value).toHashCode();
        }
    }

    /** A single Sample. */
    public static class Sample implements Serializable {
        private final double value;
        private final long timestamp;

        public Sample(double value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public double getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    private final Label[] labels;
    private final Sample[] samples;
    private final String metricName;

    public PrometheusTimeSeries(String metricName, Label[] labels, Sample[] samples) {
        this.metricName = metricName;
        this.labels = labels;
        this.samples = samples;
    }

    public Label[] getLabels() {
        return labels;
    }

    public Sample[] getSamples() {
        return samples;
    }

    public String getMetricName() {
        return metricName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderFrom(PrometheusTimeSeries other) {
        return new Builder(
                Arrays.asList(other.labels), Arrays.asList(other.samples), other.metricName);
    }

    /** Builder for sink input pojo instance. */
    public static final class Builder {
        private List<Label> labels = new ArrayList<>();
        private List<Sample> samples = new ArrayList<>();
        private String metricName;

        private Builder(List<Label> labels, List<Sample> samples, String metricName) {
            this.labels = labels;
            this.samples = samples;
            this.metricName = metricName;
        }

        private Builder() {}

        public Builder withMetricName(String metricName) {
            this.metricName = metricName;
            return this;
        }

        public Builder addLabel(String labelName, String labelValue) {
            labels.add(new Label(labelName, labelValue));
            return this;
        }

        public Builder addSample(double value, long timestamp) {
            samples.add(new Sample(value, timestamp));
            return this;
        }

        public PrometheusTimeSeries build() {
            return new PrometheusTimeSeries(
                    metricName,
                    labels.toArray(new Label[labels.size()]),
                    samples.toArray(new Sample[samples.size()]));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusTimeSeries that = (PrometheusTimeSeries) o;
        return Arrays.equals(labels, that.labels)
                && Arrays.equals(samples, that.samples)
                && Objects.equals(metricName, that.metricName);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(metricName);
        result = 31 * result + Arrays.hashCode(labels);
        result = 31 * result + Arrays.hashCode(samples);
        return result;
    }
}
