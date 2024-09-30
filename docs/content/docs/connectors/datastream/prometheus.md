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

This sink connector can be used to write **data** to Prometheus, using Prometheus [Remote-Write](https://prometheus.io/docs/specs/remote_write_spec/) interface.

{{< hint warn >}}This connector is not meant for sending internal Flink metrics to Prometheus.
To publish Flink metrics, for monitoring health and operations of the Flink cluster, you should use 
[Metric Reporters](../../../deployment/metric_reporters/).{{< /hint >}}

To use the connector, add the following Maven dependency to your project:

{{< connector_artifact flink-connector-prometheus prometheus >}}


## Usage

The Prometheus sink provides a build class for constructing instance of `PrometheusSink`. The code snippets below shows 
how to build a `PrometheusSink` also using a [request signer](#request-signer), and [customizing the error handling behavior](#error-handling-configuration).

```java
PrometheusSink sink = PrometheusSink.builder()
        .setPrometheusRemoteWriteUrl(prometheusRemoteWriteUrl)
        .setRequestSigner(new AmazonManagedPrometheusWriteRequestSigner(prometheusRemoteWriteUrl, prometheusRegion)) // Optional
        .build();
```
The only **required** configuration `prometheusRemoteWriteUrl`. All other configurations are optional.

If your sink has parallelism > 1 you need to ensure the stream is keyed to ensure all samples of the same time-series 
are in the same partition. See [Sink parallelism and keyed streams](#sink-parallelism-and-keyed-streams) for details.


### PrometheusTimeSeries input data objects

The sink expects [`PrometheusTimeSeries`](#prometheustimeseries-input-data-objects) records as input.
Converting your record into `PrometheusTimeSeries` is a responsibility of the application, using a map or flatMap operator.

`PrometheusTimeSeries` provides a [builder interface](#populating-prometheustimeseries) to create and populate instances.
Instances are immutable and cannot be reused.

Each `PrometheusTimeSeries` represents a single time-series record of the Remote-Write interface.

{{< hint info >}}
The term "time-series" in the context of Prometheus is overloaded.
It means both *a series of samples with a unique set of labels* (a time-series in the underlying time-series database),
and *a record sent to the Remote-Write interface*. A `PrometheusTimeSeries` instance represents a record sent to 
Remote-Write.

The two concepts are related, because all time-series "records" with identical set of labels are sent to the same
time-series of the datastore.
{{< /hint >}}

Each `PrometheusTimeSeries` record contains:

- One **`metricName`**. A string that translated into the value of the `__name__` label.
- Zero or more **`Label`**. Each label contains a `key` and a `value`, both `String`. 
  Labels represent additional dimensions of the samples. Duplicate Label keys are not allowed.
- One or more **`Sample`**. Each sample has a `value` (`double`) representing the measure, and
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

{{< hint info >}}The set of labels and the metric name is the unique identifier of the time-series in the datastore.{{< /hint >}}


### Populating PrometheusTimeSeries

`PrometheusTimeSeries` provides a builder interface.

```java
PrometheusTimeSeries inputRecord =
        PrometheusTimeSeries.builder()
                .withMetricName(metricName)
                .addLabel("DeviceID", instanceId)
                .addLabel("RoomID", roomId)
                .addSample(measurement1, time1)
                .addSample(measurement2, time2)
                .build();
```

Each `PrometheusTimeSeries` instance can contain multiple samples and you can call `.addSample(...)` multiple times.
The max number of samples per record is limited by the `maxBatchSizeInSamples` configuration.

Note that aggregating multiple samples into a single `PrometheusTimeSeries` record may improve write performances.


## Prometheus remote-write constraints

Prometheus imposes strict constrains on data format and ordering. Any write request violating these constraints is rejected.

For details about Prometheus Remote-Write interface constraints, see [Remote-Write specification](https://prometheus.io/docs/specs/remote_write_spec).

In practice, the behavior of the Remote-Write endpoint varies based on the Prometheus implementation and its configuration, 
and some of these constraints may be relaxed.

The Flink Prometheus connector **does not enforce** any data constraints directly. The user is responsible of sending well-formed and
in-order data to the sink, respecting the actual constraints of the Prometheus implementation in use. 
For more details, see [User responsibilities](#user-responsibilities)


### Ordering constraints

Remote-Write specification imposes ordering constrains at multiple levels:

1. **Labels** within a `PrometheusTimeSeries` record must be in lexicographical **order by `key`**.
2. **Samples** within a `PrometheusTimeSeries` record must be in **`timestamp` order**, from older to newer.
3. **All samples** belonging to the **same time-series** (a unique set of labels and metricName) must be written in **`timestamp` order**.
4. Duplicate Samples belonging to the **same time-series** and with the **same timestamp** are not allowed

If the Prometheus implementation supports [*out-of-order time window*](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#tsdb), and
and the option is enabled, the third constraint above is relaxed. You can send out-of-order samples within the configurable
time window.


### Format constraints

The `PrometheusTimeSeries` records sent to the sink must respect the following constraints:

* **`metricName`** must be defined and non-empty. The connector translates this property into the value of the `__name__` label.
* Label **names** must follow the regex `[a-zA-Z:_]([a-zA-Z0-9_:])`.
* Label **names** must not begin with `__` (double underscore). These label names are reserved.
* Label **values** and `metricName` may contain any UTF-8 character.
* Label **values** cannot be empty (null or empty string).

Records violating these constraints may be rejected by Prometheus.


### User responsibilities

The user is responsible for sending to the sink records (`PrometheusTimeSeries`) respecting format and ordering 
constraints of your Prometheus installation. The connector does not perform any validation or reordering.

Sample ordering by timestamp is particularly important. 
Samples belonging to the same time-series, i.e. with the same set of Labels and metric name, must be in timestamp order.
Data must be generated in order, and order must be retained upstream of the sink. This usually implies partitioning records by the combination 
of all Labels and metric name.

Any record violating ordering sent to the sink is dropped and may cause other records batched in the same write-request to be dropped. 
See [Error handling](#error-handling) for more details.


### Sink parallelism and keyed streams

Each sink operator sub-task uses a single thread to send write requests to the Prometheus Remote-Write endpoint.
`PrometheusTimeSeries` records are written in the order they are received by the sub-task. However, sending to 
different sub-tasks  records belonging to the same time-series (i.e. `PrometheusTimeSeries` with identical set of labels
and metric name) may result in out-of-order writes, rejected by Prometheus.

The sink allows parallelism > 1 to scale writes. To prevent from introducing accidental out-of-order writes, the input 
of the sink must be keyed ensuring that all records belonging to the same time-series must be handled by the same sink sub-task.

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
However, the application is still responsible to ensure that all samples with the same set of labels and metric name are sent 
to the sink in order of timestamp and by the same partition.


## Error handling

This paragraph covers handling of errors conditions when writing data to the Remote-Write endpoint. 

There are three types of error conditions:

1. Retryable errors due to temporary error conditions in the Remote-Write server or due to throttling: `5xx` or `429` http responses, connectivity issues.
2. Non-retryable errors due to data violating any of the constraints, malformed data or out-of-order samples: `4xx` http responses, except `429`, `403` and `404`.
3. Fatal error response: authentication failures (`403` http response) or incorrect endpoint path (`404` http response).
4. Any other unexpected failure while writing, due to exceptions while writing to the Prometheus endpoint.


### On-error behaviors

The connector implements two types of behavior on error:

1. `FAIL`: throw an unhandled exception, cause the job to fail;
2. `DISCARD_AND_CONTINUE`: discard the request that caused the error and continue with the next request.

When a write request is discarded by the `DISCARD_AND_CONTINUE` behavior, the following actions happen:

1. Log a message at `WARN` level with the cause of the error. When the error is caused by a response of the Remote-Write
    endpoint, the log entry contains the message returned by the endpoint.
2. Increase one connector [counters](#connector-metrics) metrics, to count the number of rejected samples and write requests.
3. **Drop the entire write request**. Note that, due to [batching](#batching), a write request may contain multiple 
    `PrometheusTimeSeries`.
4. Continue with the next input record.


{{< hint info >}}Prometheus Remote-Write does not support partial failures. 
Due to connector [batching](#batching), a single write request may contain multiple input records (`PrometheusTimeSeries`). 
When a write request is discarded for any reason, all records in the batch are dropped. Even if the error was caused by a single malformed record in the write request.{{< /hint >}}


#### Retriable error responses

A typical retriable error condition Prometheus throttling, with a `429, Too Many Requests` response.

On retriable errors, the connector will retry, using a [configurable backoff strategy](#retry-configuration) until a maximum number of retries, then fails.
What happens at this point depends on the `onMaxRetryExceeded` [error handling configuration](#error-handling-configuration).

* `onMaxRetryExceeded` is `FAIL` (default): the job fails and restarts from checkpoint.
* `onMaxRetryExceeded` is `DISCARD_AND_CONTINUE`: the entire write request (the batch) is dropped and the sink continue with the next record. 


#### Non retriable error responses

A typical non-retriable condition is due to malformed or out of order data rejected by Prometheus with a `400, Bad Request` response.

When such an error response is received, the connector applies the `DISCARD_AND_CONTINUE` behavior.
This behavior is currently not configurable.

{{< hint info >}}Due to discarding records on non-retriable errors make the sink **at most once**.
This is not a limitation of the sink implementation, but rather the behavior expected by Prometheus.
Causing the job to fail on errors caused by malformed data, would restart the job from checkpoint and cause the error again, putting the application in an endless loop.{{< /hint >}}

#### Fatal error responses

`403, Forbidden` responses, caused by incorrect or missing authentication, and `404, Not Found` responses, caused by incorrect endpoint URL, are always considered fatal (behavior is always `FAIL`, not configurable).

#### Other I/O errors

Any I/O error while connecting to the endpoint is also fatal (behavior is always `FAIL`, not configurable).


### Error handling configuration

The error handling behavor is partly configurable. 

You can change the error handling behavior when building the instance of the sink.

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setErrorHandlingBehaviourConfiguration(
                SinkWriterErrorHandlingBehaviorConfiguration.builder()
                    .onMaxRetryExceeded(OnErrorBehavior.DISCARD_AND_CONTINUE)
                    .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
                    .build())
        .build();
```

At the moment, the only supported configuration is when the maximum number of retries is exceedeed (`onMaxRetryExceeded`), after 
a [retriable error](#retriable-error-responses). The default behavior is `FAIL`.

The configuration also allows setting the behavior on Prometheus non-retriable error (`onPrometheusNonRetriableError`), but the only 
allowed value at the moment is `DISCARD_AND_CONTINUE`. 


### Retry configuration

When a retriable error condition is encoutered, for example a `5xx` or `429` response, the sink retries with an exponential backoff strategy.

You can control the retry strategy with the following configurations:

* `initialRetryDelayMS`: (default `30` millis) Initial retry delay. Retry delay doubles on every subsequent retry up to the maximum retry delay.
* `maxRetryDelayMS`: (default `5000` millis) Maximum retry delay. When this delay is reached, every subsequent retry has the same delay. Must be bigger than `InitialRetryDelayMS`
* `maxRetryCount`: (default `100`) Maximum number of retries for a single write request. To (practically) retry forever set max retries to `Integer.MAX_VALUE`.

When `maxRetryCount` is exceeded, the connector stops retrying. What happens at this point depends on the `onMaxRetryExceeded` 
[error handling behavior](#error-handling-configuration).

You can configure the retry strategy when building the sink.

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

To optimize write throughput, the sink batches writes. Multiple `PrometheusTimeSeries` are bathed in a single write request
to the Remote-Write endpoint.

Batching is based on the number of **samples** (not bytes) and on max buffering time.

The connector starts with writing single `PrometheusTimeSeries` per write-request. If writes succeeds, the batch size is 
increased up to a configurable limit of samples per write-request. The actual number of `PrometheusTimeSeries` per write 
request varies, because a `PrometheusTimeSeries` may contain a variable number of samples.

Buffered `PrometheusTimeSeries` are stored in Flink state, and are not lost on application restart.

`PrometheusTimeSeries` order of PrometheusTimeSeries` is retained, per sub-task. Each sub-task has at most one in-flight 
write request. A write request is retried until it completely succeeds or fails.

You can control batching with the following parameters:

* `maxBatchSizeInSamples`: (default: `500`) max number of samples in a write request.
* `maxTimeInBufferMS`: (default: `5000` millis) Max time `PrometheusTimeSeries` are buffered before sending the write-request.
* `maxRecordSizeInSamples`: (default: `500`) max number of samples in a single `PrometheusTimeSeries`. It must be less or equal `MaxBatchSizeInSamples`.

You can configure batching when building the sink.

```java
PrometheusSink sink = PrometheusSink.builder()
        .setMaxBatchSizeInSamples(100)
        .setMaxTimeInBufferMS(10000)
        // ...    
        .build();
```

{{< hint info >}}
Larger batches improve write performance but also increases the number of record lost when a write request is rejected, due to `DISCARD_AND_CONTINUE`
behavior.
Reduging `maxBatchSizeInSamples` minimize data loss in this cases, but also heavly reduce the throughput that Prometheus can injest.
The default `maxBatchSizeInSamples` of 500 maximizes the ingestion throughput.{{< /hint >}}

{{< hint warn >}}If any record containing more samples than `maxRecordSizeInSamples`, the sink with throw and exception and the job 
will fail and restart continuously.{{< /hint >}}

## Request Signer

Remote-Write specification [does not specify any authentication scheme](https://prometheus.io/docs/specs/remote_write_spec/#out-of-scope).
Any authentication is delegated to the specific Prometheus implementation.

The connector allows to specify a request signer. The signer can be used to add HTTP headers to the requests. These headers can 
be based on the request body or any of the existing headers.
A signer can be used to add to the http requests custom authorization tokens or signature tokens.

You can implement your own request signer implementing the `PrometheusRequestSigner` interface. 

```java
public interface PrometheusRequestSigner extends Serializable {
  void addSignatureHeaders(Map<String, String> requestHeaders, byte[] requestBody);
}
```

Refer to the JavaDoc or the source code for more details.

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

{{< connector_artifact flink-connector-prometheus prometheus-request-signer-amp >}}

The AMP signer retrieves AWS credentials using 
[`DefaultCredentialsProvider`](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)
and uses the credentials to sign every request to the Remote-Write endpoint.

You can add the AMP request signer to the sink:

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setRequestSigner(new AmazonManagedPrometheusWriteRequestSigner(prometheusRemoteWriteUrl, prometheusRegion))
        .build();
```

## HTTP client configuration

You can configure the HTTP client that sends write requests to the Remote-Write endpoint.

* `socketTimeoutMs`: (default: `5000` millis) HTTP client socket timeout
* `httpUserAgent`: (default: `Flink-Prometheus`) User-Agent header

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setSocketTimeoutMs(5000)
        .setHttpUserAgent(USER_AGENT)
        .build();
```

## Connector metrics

The connector exposes custom metrics, counting data successfully written to the Remote-Write endpoint, and also data dropped
due to [error handling configuration](#error-handling-configuration)  set to `DISCARD_AND_CONTINUE`.


| Metric name                     | Description                                                                                                |
|---------------------------------|------------------------------------------------------------------------------------------------------------|
| `numSamplesOut`                 | Count of **samples** successfully written to Prometheus                                                    |
| `numWriteRequestsOut`           | Count of **samples** successfully written to Prometheus                                                    |
| `numWriteRequestsRetries`       | Count of **write requests** reties, due to retryable errors (e.g. throttling)                              |
| `numSamplesDropped`             | Count of  **samples** that have been dropped (data loss!) due to any `DISCARD_AND_CONTINUE` error handling |
| `numSamplesNonRetriableDropped` | Count of **samples** that have been dropped (data loss!) due to `onPrometheusNonRetriableError` set to `DISCARD_AND_CONTINUE` (default) |
| `numSamplesRetryLimitDropped`   | Count of **samples** that have been dropped (data loss!) due to `onMaxRetryExceeded` set to `DISCARD_AND_CONTINUE`, when the retry limit was exceeded |
| `numWriteRequestsPermanentlyFailed` | Count of **write requests** permanently failed, due to any reasons |


{{< hint info >}}The `numByteSend` metric should be ignored. This metric does not actually measure bytes, due to limitations 
of AsyncSink this connector is based on. Use `numSamplesOut` and `numWriteRequestsOut` to monitor the actual output of 
the sink.{{< /hint >}}

The metric group name is "Prometheus" by default. It can be configured.

```java
PrometheusSink sink = PrometheusSink.builder()
        // ...    
        .setMetricGroupName("my-metric-group")
        .build();
```

## Example application

You can find a complete application demonstrating the configuration and usage of this sink in the tests of the connector.

Check out the source of `org.apache.flink.connector.prometheus.sink.examples.DataStreamExample`.

This is not a real test. This class contains a full application that generates random data internally and writes to Prometheus.

## Connector guarantees

The connector provides **at-most-once**. Data loss can happen, in particular if input data is malformed or out of order.

Data may be lost due to the `DISCARD_AND_CONTINUE` [on-error behaviour](#on-error-behaviors). This behavior may optionally be enabled 
when the maximum number of retries is exceeded, but is always enabled when a non-retriable error condition is encountered.

This is not a limitation of this sink, but it's due to the design of Prometheus remote-write interfaces, strictly 
not allowing out-of-order writes in the same time-series. Failing when a write is rejected by Prometheus would only put the job in 
an endless loop, failing, restarting from checkpoint, and failing again when the write is inevitably rejected.

At the same time, Prometheus imposes per time-series ordering by timestamp. The sink guarantees the order is retained per partition.
Key by using `PrometheusTimeSeriesLabelsAndMetricNameKeySelector` before the sink guarantees input is partitioned by time-series, and
no accidental reordering happens during the write. It is user's responsibility to retain order upstream of the sink. Any out of order 
record is rejected by Prometheus


{{< top >}}
