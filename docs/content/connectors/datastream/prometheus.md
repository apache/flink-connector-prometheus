---
title: Prometheus
weight: 5
type: docs
aliases:
  - /dev/connectors/prometheus.html
  - /apis/streaming/connectors/prometheus.html
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Prometheus Sink

This sink connector can be used to write **data** to Prometheus, using the [Remote-Write](https://prometheus.io/docs/specs/remote_write_spec/) interface.

{{< hint warn >}}This connector is not meant for sending internal Flink metrics to Prometheus.
[Metric Reporters](../../../deployment/metric_reporters/) should be used for monitoring health and operations of the Flink cluster and job.{{< /hint >}}

To use the connector, add the following Maven dependency to your project:

{{< connector_artifact flink-connector-prometheus prometheus >}}

## Using the Prometheus sink

The only mandatory parameter for the sink is the Prometheus Remote-Write URL.
All other parameters are optional. 

You probably also want to customize the [configurable error handling](#error-handling-configuration) behavior. 
Refer to [Design consideration](#design-considerations) and [Error handling](#error-handling) to understand the implication
on data consistency and job availability.

You also have to provide a [Request signer](#request-signer) to support Prometheus Remote-Write endpoint authentication.

```java
PrometheusSink sink = PrometheusSink.builder()
        .setPrometheusRemoteWriteUrl(prometheusRemoteWriteUrl)
        .setRequestSigner(new AmazonManagedPrometheusWriteRequestSigner(prometheusRemoteWriteUrl, prometheusRegion)) // Optional
        .setErrorHandlingBehaviourConfiguration( // Optional
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
                        .onMaxRetryExceeded(OnErrorBehavior.FAIL)
                        .onHttpClientIOFail(OnErrorBehavior.FAIL)
                        .build())
        .build();
```

The sink expects [`PrometheusTimeSeries`](#prometheustimeseries-input-data-objects) records as input.
It is responsibility of the application [converting any record into `PrometheusTimeSeries`](#populating-prometheustimeseries), for example
using a map or flatMap operator.

If the sink operator has parallelism greater than 1, the stream sent to the sink must be [keyed using `PrometheusTimeSeriesLabelsAndMetricNameKeySelector`](#sink-parallelism-and-keyed-streams) 
to ensure all samples of the same time series are written by the same sink operator sub-task.
Failing to partition the stream correctly may cause Prometheus rejecting some writes for violating the strict 
[ordering constraints](#prometheus-remote-write-constraints) imposed by the 
[Remote-Write](https://prometheus.io/docs/specs/remote_write_spec/#ordering) interface.


### PrometheusTimeSeries input data objects

The sink accepts `PrometheusTimeSeries` data objects as input.

`PrometheusTimeSeries` is immutable. Instances cannot be reused. 

You can use the [builder interface](#populating-prometheustimeseries) to create and populate `PrometheusTimeSeries` instances.

Each `PrometheusTimeSeries` represents a single time-series record of the Remote-Write interface.

{{< hint info >}}
The term "time series" is overloaded, in the context of Prometheus.
It means both *a series of samples with a unique set of labels* (a time-series in the underlying time-series database),
and *a record sent to the Remote-Write interface*. 
A `PrometheusTimeSeries` instance represents a record sent to the Remote-Write interface.

The two concepts are related, because all time-series "records" with identical set of labels are sent to the same
time-series of the datastore, but they must not be confused.
{{< /hint >}}

Each `PrometheusTimeSeries` record contains:

- **One `metricName`**. A string that is translated into the value of the `__name__` label of the time-series record sent to Prometheus.
- **Zero or more `Label`**. Each label contains a `key` and a `value`, both `String`. 
  Labels represent additional dimensions of the samples. Duplicate keys are not allowed.
- **One or more `Sample`**. Each sample has a `value` (`double`) representing the measure, and
  a `timestamp` (`long`) representing the time of the measure, in milliseconds from the Epoch.

The following pseudocode represents the structure of a `PrometheusTimeSeries` record:

```
PrometheusTimeSeries
  + --> (1) metricName <String>
  + --> (0..*) Label
            + name <String>
            + value <String>
  + --> 1..* Sample
            + timestamp <long>
            + value <double>   
```

{{< hint info >}}The set of labels and the metric name, that becomes an additional label with name `__name__`, is the
unique identifier of the time-series in the datastore.{{< /hint >}}

### Populating PrometheusTimeSeries

`PrometheusTimeSeries` provides a builder interface.

```java
PrometheusTimeSeries inputRecord =
        PrometheusTimeSeries.builder()
                .withMetricName(metricName)
                .addLabel("DeviceID", instanceId)
                .addLabel("RoomID", roomId)
                .addSample(measurement, time)
                .build();
```

Note that each `PrometheusTimeSeries` instance can contain multiple samples. You can call `.addSample(...)` multiple times

Aggregating multiple measurements (samples) in a single `PrometheusTimeSeries` record may improve the write performances.

## Prometheus remote-write constraints

The Prometheus [Remote-Write specification](https://prometheus.io/docs/specs/remote_write_spec) imposes strict
constrains about data format and ordering, for samples and labels in a single time-series record. 
It also imposes [strict ordering](https://prometheus.io/docs/specs/remote_write_spec/#ordering) for the writes to the same time-series.
Any write request violating these constraints is rejected and [cannot be retried](https://prometheus.io/docs/specs/remote_write_spec/#retries-backoff).

In practice, the behavior of the Remote-Write endpoint may chance based on the Prometheus implementation and its configuration.
Some fo these constraints may be relaxed.

For these reasons the connector **does not enforce** any of these constraints. 
It is [responsibility of the application](#application-responsibilities) sending well-formed data and respecting ordering constraints.


### Ordering constraints

Remote-Write specification imposes multiple levels of ordering:

1. Labels within a `PrometheusTimeSeries` record must be in lexicographical order by `key`.
2. Samples within a `PrometheusTimeSeries` record must be in `timestamp` order, from older to newer.
3. All samples sent to a time-series (a unique set of labels and metricName) must be written in `timestamp` order

If [*out-of-order time window*](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#tsdb) is
supported by the Prometheus implementation and enabled, you can send out-of-order samples within an acceptable time window.

### Format constraints

`PrometheusTimeSeries` records should respect these constraints:

* `metricName` must be defined and non-empty. The connector translates this property into the value of the `__name__` label.
* Label **names** must follow the regex `[a-zA-Z:_]([a-zA-Z0-9_:])`.
* Label **names** must not begin with `__` (double underscore). These label names are reserved.
* Label **values** and `metricName` may contain any UTF-8 character.
* Label **values** cannot be empty (null or empty string).

`PrometheusTimeSeries` records violating these constraints may be rejected by Prometheus on write.

### Application responsibilities

The application has the responsibility of sending to the sink `PrometheusTimeSeries` records that
respect format and ordering constraints.

The connector does not perform any validation, of format and ordering.

Depending on the Prometheus implementation and configuration, the Remote-Write endpoint may enforce these constraints, 
rejecting any offending write request.

You can control the behavior of the connector when a request is rejected.
See [Error handling](#error-handling) for details.

### Sink parallelism and keyed streams

Each sink operator sub-task uses a single thread to send write requests to the Prometheus Remote-Write endpoint.
`PrometheusTimeSeries` records are written in the order they are received by the sub-task.
However, multiple sub-tasks writing samples of the same time-series (`PrometheusTimeSeries` with identical set of labels 
and metric name) may result in out-of-order writes rejected by Prometheus.

You can set the sink parallelism > 1 to scale writes.
To prevent multiple writer threads from introducing accidental out-of-order, the input to the sink must be
keyed. All records belonging to the same time-series must be handled by the same sink sub-task.

You can achieve this with a `keyBy()` using `PrometheusTimeSeriesLabelsAndMetricNameKeySelector` as key selector.

```java

DataStream<MyRecord> inputRecords;
// ...
KeyedStream<PrometheusTimeSeries> timeSeries = inputRecords
        .map(new MyRecordToTimeSeriesMapper())
        .keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector());

timeSeries.sinkTo(prometheusSink);
```

Keying the input using the key selector prevents accidental out-of-orderness due to repartitioning before the sink operator.
However, it is application responsibility to ensure that all records with the same set of labels and metric name are
processed by the same partition and in order of sample timestamp.

## Error handling

This paragraph covers handling of errors conditions that may occur when writing data to the Remote-Write endpoint. 

There are three types of error conditions:

1. Retryable errors due to temporary error conditions in the Remote-Write server or due to throttling (endpoint responds with `5xx` or `429` http status).
2. Non-retryable errors due to data violating the constraints, malformed data or out-of-order samples, or authentication failures (endpoint responds with any `4xx` http status, except `429`).
3. Other I/O errors on the connection to the endpoint, for example a connection timeout (impossible to get any response from the endpoint).

Remote-Write does not support partial failures. 
If a write request contains offending data, the entire write request is rejected with a non-retryable error.
Due to sink [batching](#batching) a single write request may contain multiple samples, also for multiple time-series.

The connector retries any retryable error, using a [configurable backoff strategy](#retry-configuration).

The connector default behavior on non-retryable and I/O error is throwing an unhandled exception that causes the job to fail.
Malformed data may become a *death pill*. The job restarts from the last checkpoint and retries writing the same data again, 
potentially in an endless loop.

To prevent this situation, you can configure the behavior of the connector on non-retryable and I/O errors.

### Error handling configuration

You can configure how the connector behaves on three categories of error conditions:

* `onPrometheusNonRetriableError`: when the endpoint responds with non-retryable error, for example due to malformed or out-of-order writes.
* `onMaxRetryExceeded`: when a retryable error is retried too many times, exceeding the max number of retries defined in the [retry configuration](#retry-configuration).
* `onHttpClientIOFail`: when the HTTP client is unable to connect to the endpoint or reports a generic I/O error, for example a connection timeout.

For each of these error conditions, you can choose between these two behaviors:

* `FAIL`: (**default behavior**) throw an unhandled exception; the job fails. 
* `DISCARD_AND_CONTINUE`: Discard the offending write request and continue

When `DISCARD_AND_CONTINUE` is selected, on error the connector behaves in the following way: 

1. Log a message at `WARN` level with the cause of the error. It contains the message returned by the endpoint or the I/O error.
2. Increase one or more of the [counters](#connector-metrics) counting the number of rejected samples and write requests.
3. Drop the entire write request. The entire write request is dropped, regardless only a few samples may violate constraints. 
   Due to [batching](#batching), a write request may contain multiple `PrometheusTimeSeries`.
4. Continue with the next input record

See [Design considerations](#design-considerations) for further considerations about error handling and connector guarantees.

You can set the error handling behavior configuration when building the sink.

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setErrorHandlingBehaviourConfiguration(
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                    .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
                    .onMaxRetryExceeded(OnErrorBehavior.DISCARD_AND_CONTINUE)
                    .onHttpClientIOFail(OnErrorBehavior.DISCARD_AND_CONTINUE)
                    .build())
        .build();
```

Any condition not configured defaults to `FAIL`.

### Retry configuration

When the Prometheus Remote-Write endpoint reports a retryable error (`5xx` and `429` https status), the connector retries with 
an exponential backoff strategy.

You can control the behavior of the retry strategy:

* `initialRetryDelayMS`: (default `30` millis) Initial retry delay. Retry delay doubles on every subsequent retry up to the maximum retry delay.
* `maxRetryDelayMS`: (default `5000` millis) Maximum retry delay. When this delay is reached, every subsequent retry has the same delay. Must be bigger than `InitialRetryDelayMS`
* `maxRetryCount`: (default `100`) Maximum number of retries for a single write request. To (practically) retry forever set max retries to `Integer.MAX_VALUE`.

When `maxRetryCount` is exceeded, the connector stops retrying.
The behavior at this point is determined by the `onMaxRetryExceeded` [error handling configuration](#error-handling-configuration).

You can configure the retry when building the sink.

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...
        .setRetryConfiguration(
                RetryConfiguration.builder()
                    .setInitialRetryDelayMS(10L)
                    .setMaxRetryDelayMS(5000L)              
                    .setMaxRetryCount(Integer.MAX_VALUE)                  
                .build())
        // ...    
        .build();
```

## Batching

To optimize write throughput, the sink batches writes, emitting write requests containing multiple `PrometheusTimeSeries`.

Batching is based on the number of **samples** (not bytes) and on buffering time.

A batch is written to the endpoint as a single [write request](https://prometheus.io/docs/specs/remote_write_spec/#protocol).
Batching starts with writing a single `PrometheusTimeSeries` per write-request, and increases up to a configurable limit of samples.
The number of `PrometheusTimeSeries` per write request varies, because a `PrometheusTimeSeries` may contain multiple samples.

`PrometheusTimeSeries` order is strictly retained, per sub-task. 
Each sub-task has at most one in-flight write request, that is retried until it completely succeeds or fails.

You can control batching with the following parameters:

* `maxBatchSizeInSamples`: (default: `500`) max number of samples in a write request.
* `maxTimeInBufferMS`: (default: `5000` millis) Max time `PrometheusTimeSeries` are buffered before sending the write-request.
* `maxRecordSizeInSamples`: (default: `500`) max number of samples in a single `PrometheusTimeSeries`. It must be less or equal `MaxBatchSizeInSamples`.

`PrometheusTimeSeries` are buffered, and written when the number of contained samples reaches `maxBatchSizeInSamples`,
or when buffering time exceeds `maxTimeInBufferMS`, whichever comes first.

Buffered `PrometheusTimeSeries` are stored in Flink state, and are not lost on application restart.

You can configure batching when building the sink.

```java
PrometheusSink sink = PrometheusSink.builder()
        .setMaxBatchSizeInSamples(100)
        .setMaxTimeInBufferMS(10000)
        // ...    
        .build();
```

{{< hint info >}}
Larger batches improve write performance but also increase the amount of potential samples lost when using `DISCARD_AND_CONTINUE` error handling behavior.

Setting `maxBatchSizeInSamples` to 1 minimizes data loss, but heavily reduce the throughput you can potentially ingest into Prometheus.

The default of 500 `maxBatchSizeInSamples` optimizes for throughput.
{{< /hint >}}


## Request Signer

Remote-Write specification [does not specify any authentication scheme](https://prometheus.io/docs/specs/remote_write_spec/#out-of-scope).
The authentication is delegated to the Prometheus implementation.

The connector allows to specify a request signer that can add custom http headers to the request, based on the request 
body and existing headers. It can be used to add custom authorization tokens or signature.

The request signer is an implementation of `PrometheusRequestSigner`. Refer to the JavaDoc or the source code for more details.

```java
public interface PrometheusRequestSigner extends Serializable {
  void addSignatureHeaders(Map<String, String> requestHeaders, byte[] requestBody);
}
```

You can add a request signer when building the sink:

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setRequestSigner(requestSigner)
        .build();
```


### Amazon Managed Prometheus request signer

An implementation of `PrometheusRequestSigner` that supports [Amazon Managed Prometheus](https://aws.amazon.com/prometheus/) (AMP)
is provided.

To use the AMP request signer, add this additional dependency to the Maven project:

{{< connector_artifact flink-connector-prometheus prometheus-request0signer-amp >}}

The AMP signer retrieves AWS credentials using [`DefaultCredentialsProvider`](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)
and use them to sign every request to the Remote-Write endpoint.

You can add the AMP request signer to the sink:

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setRequestSigner(new AmazonManagedPrometheusWriteRequestSigner(prometheusRemoteWriteUrl, prometheusRegion))
        .build();
```


## HTTP client configuration

You can configure the HTTP client that sends write requests to the Remote-Write endpoint.

* `SocketTimeoutMs`: (default: `5000` millis) HTTP client socket timeout
* `HttpUserAgent`: (default: `Flink-Prometheus`) User-Agent header

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setSocketTimeoutMs(5000)
        .setHttpUserAgent(USER_AGENT)
        .build();
```

## Connector metrics

The connector exposes a number of custom metrics.
In particular, metrics counting the data dropped when [error handling configuration](#error-handling-configuration) is set to `DISCARD_AND_CONTINUE`, can be used to measure the data loss due to errors.


* `numSamplesOut`: count of **samples** successfully written to Prometheus.
* `numWriteRequestsOut`: count of **write requests** successfully completed.
* `numWriteRequestsRetries`: count of **write requests** reties, due to retryable errors (e.g. throttling).
* `numSamplesDropped`: count of  **samples** that have been dropped (data loss!) due to any `DISCARD_AND_CONTINUE` error handling.
* `numSamplesNonRetriableDropped`: count of **samples** that have been dropped (data loss!) due to `onPrometheusNonRetriableError` set to `DISCARD_AND_CONTINUE`.
* `numSamplesRetryLimitDropped`: count of **samples** that have been dropped (data loss!) due to `onMaxRetryExceeded` set to `DISCARD_AND_CONTINUE`, when the retry limit was exceeded.
* `numWriteRequestsPermanentlyFailed`: count of **write requests** permanently failed, due to any reasons

{{< hint info >}}The `numByteSend` metric should be ignored. 
This metric does not actually measure bytes, due to limitations of AsyncSink this connector is based on. 
Use `numSamplesOut` and `numWriteRequestsOut` to monitor the actual output of the sink.{{< /hint >}}

The metric group name is "Prometheus" by default. It can be configured.

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setMetricGroupName("my-metric-group")
        .build();
```


## Design considerations

By design, Prometheus optimizes fast ingestion over data completeness. 
Prometheus Remote-Write protocol imposes strict constrains on the ordering of writes
Also, Remote-Write does not support partial failures and offending write requests are completely rejected, regardless
they may contain only few offending data points.

Conversely, Flink is primarily designed for data consistency. 
The default behavior of a Flink connector is normally "fail rather than dropping data".

The default behavior of this connector reflects the general Flink approach. 
Any error that may potentially cause data loss, causes the job to fail.

The side effect is that malformed input data becomes a *poison-pill*, causing the job to continuously fail and restart from checkpoint.
This may not be the behavior you expect for a pipeline handling observability data.

For this reason, the connector [error handling](#error-handling) behavior is configurable. 
In many cases, you might prefer `DISCARD_AND_CONTINUE` over the default `FAIL`, prioritizing availability and data 
freshness over data consistency. 

Note that, when `DISCARD_AND_CONTINUE` is selected, the sink does no longer provide at-least-once guarantees.

When you choose `DISCARD_AND_CONTINUE`, you should monitor the actual volume of dropped data
with the [custom metrics](#connector-metrics) exposed by the connector. 

You can also find details about what caused data being dropped looking at the `WARN` log lines emitted every time the 
sink discards a write request due to an unrecoverable condition.


## Example application

You can find a complete application demonstrating the configuration and usage of this sink in the tests of the connector.

Check out the source of `org.apache.flink.connector.prometheus.sink.examples.DataStreamExample`.

This is not a real test. This class contains a full application that generates random data internally and writes to Prometheus.

{{< top >}}
