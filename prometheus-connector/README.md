## Flink Prometheus connector (sink)

Implementation of the Prometheus sink connector for DataStream API.

The sink writes to Prometheus using the Remote-Write interface, based on [Remote-Write specifications version 1.0](https://prometheus.io/docs/concepts/remote_write_spec/)

### Guarantees and input restrictions

Due to the strict [ordering](https://prometheus.io/docs/concepts/remote_write_spec/#ordering) and [format](https://prometheus.io/docs/concepts/remote_write_spec/#labels) requirements
of Prometheus Remote-Write, the sink guarantees that input data are written to Prometheus only if input data are in order and well-formed.

For efficiency, the connector does not do any validation.
If input is out of order or malformed, the write request is rejected by Prometheus and data is discarded by the sink.
The connector will log a warning and count rejected data in custom metrics, but the data is discarded.

The sink receives as input time-series, each containing one or more samples. 
To optimise the write throughput, input time-series are batched, in the order they are received, and written with a single write-request.

If a write-request contains any out-of-order or malformed data, **the entire request is rejected** and all time series are discarded.
The reason is Remote-Write specifications [explicitly forbids retrying](https://prometheus.io/docs/concepts/remote_write_spec/#retries-backoff) of rejected write requests (4xx responses).
and the Prometheus response does not contain enough information to efficiently partially retry the write, discarding the offending data.

### Responsibilities of the application

It is responsibility of the application sending the data to the sink in the correct order and format.

1. Input time-series must be well-formed, e.g. only valid and non-duplicated labels, 
samples in timestamp order (see [Labels and Ordering](https://prometheus.io/docs/concepts/remote_write_spec/#labels) in Prometheus Remote-Write specs).
2. Input time-series with identical labels are sent to the sink in timestamp order.
3. If sink parallelism > 1 is used, the input stream must be partitioned so that all time-series with identical labels go to the same sink subtask. A `KeySelector` is provided to partition input correctly (see [Partitioning](#partitioning), below). 


#### Sink input objects

To help sending well-formed data to the sink, the connector expect [`PrometheusTimeSeries`](./src/main/java/org/apache/flink/connector/prometheus/sink/PrometheusTimeSeries.java) POJOs as input.

Each `PrometheusTimeSeries` instance maps 1-to-1 to a [remote-write `TimeSeries`](https://prometheus.io/docs/concepts/remote_write_spec/#protocol). Each object contains:
* exactly one `metericName`, mapped to the special  `__name__` label
* optionally, any number of additional labels { k: String, v:String }
* one or more `Samples` { value: double, timestamp: long } - must be in timestamp order

`PrometheusTimeSeries` provides a builder interface.

```java

// List<Tuple2<Double, Long>> samples = ...

PrometheusTimeSeries.Builder tsBuilder = PrometheusTimeSeries.builder()
    .withMetricName("CPU") // mapped to  `__name__` label
    .addLabel("InstanceID", instanceId)
    .addLabel("AcccountID", accountId);
    
for(Tuple2<Double, Long> sample : samples) {
    tsBuilder.addSample(sample.f0, sample.f1);
}

PrometheusTimeSeries ts = tsBuilder.build();
```


**Important**: for efficiency, the builder does reorder the samples. It is responsibility of the application to **add samples in timestamp order**.

### Batching, blocking writes and retry

The sink batches multiple time-series into a single write-request, retaining the order..

Batching is based on the number of samples. Each write-request contains up to 500 samples, with a max buffering time of 5 seconds 
(both configurable). The number of time-series doesn't matter.

As by [Prometheus Remote-Write specifications](https://prometheus.io/docs/concepts/remote_write_spec/#retries-backoff), 
the sink retries 5xx and 429 responses. Retrying is blocking, to retain sample ordering, and uses and exponential backoff.

The exponential backoff starts with an initial delay (default 30 ms) and increases it exponentially up to a max retry 
delay (default 5 sec). It continues retrying until the max number of retries is reached (default reties forever).

On non-retriable error response (4xx, except 429, non retryable exceptions), or on reaching the retry limit, 
**the entire write-request**, containing the batch of time-series, **is dropped**.

Every dropped request is logged at WARN level, including the reason provided by the remote-write endpoint.

### Initializing the sink

Example of sink initialisation (for documentation purposes, we are setting all parameters to their default values):

```java
PrometheusSink sink =  PrometheusSink.builder()
    .setMaxBatchSizeInSamples(500)              // Batch size (write-request size), in samples (default: 500)
    .setMaxRecordSizeInSamples(500)             // Max sink input record size, in samples (default: 500), must be <= maxBatchSizeInSamples
    .setMaxTimeInBufferMS(5000)                 // Max time a time-series is buffered for batching (default: 5000 ms)
    .setRetryConfiguration(RetryConfiguration.builder()
        .setInitialRetryDelayMS(30L)            // Initial retry delay (default: 30 ms)
        .setMaxRetryDelayMS(5000L)              // Maximum retray delay, with exponential backoff (default: 5000 ms)
        .setMaxRetryCount(100)                  // Max number of retries (default: 100)
        .build())
    .setSocketTimeoutMs(5000)                   // Http client socket timeout (default: 5000 ms)
    .setPrometheusRemoteWriteUrl(prometheusRemoteWriteUrl)  // Remote-write URL
    .setRequestSigner(new AmazonManagedPrometheusWriteRequestSigner(prometheusRemoteWriteUrl, prometheusRegion)) // Optional request signed (AMP request signer in this example)
    .setErrorHandlingBehaviourConfiguration(SinkWriterErrorHandlingBehaviorConfiguration.builder()
         .onPrometheusNonRetriableError(OnErrorBehavior.FAIL)
         .onMaxRetryExceeded(OnErrorBehavior.FAIL)
         .onHttpClientIOFail(OnErrorBehavior.FAIL)
         .build())
    .build();
```

### Partitioning

When the sink has parallelism > 1, the stream must be partitioned so that all time-series with same labels go to the same
sink operator sub-task. If this is not the case, samples may be written out-of-order, and be rejected by Prometheus.

A `keyBy()` using the provided key selector, 
[`PrometheusTimeSeriesLabelsAndMetricNameKeySelector`](./src/main/java/org/apache/flink/connector/prometheus/sink/PrometheusTimeSeriesLabelsAndMetricNameKeySelector.java), automatically partitions the time-series by labels.

### Authentication and Request Signing (optional)

The sink supports optional request-signing for authentication, implementing the 
[`PrometheusRequestSigner`](./src/main/java/org/apache/flink/connector/prometheus/sink/PrometheusRequestSigner.java)
interface.

### Retriable errors

The sink complies with Prometheus remote-write specs not retrying any request that return status codes `4xx`, except `429`, 
and retrying requests that return `5xx` or `429`, with an exponential backoff strategy.

The retry strategy can be configured, as shown in the following snippet:

```java
PrometheusSink sink =  PrometheusSink.builder()
    .setRetryConfiguration(RetryConfiguration.builder()
        .setInitialRetryDelayMS(30L)            // Initial retry delay (default: 30 ms)
        .setMaxRetryDelayMS(5000L)              // Maximum retray delay, with exponential backoff (default: 5000 ms)
        .setMaxRetryCount(100)                  // Max number of retries (default: 100)
        .build())
    // ...    
    .build();
```

### Error handling behavior

The behaviour of the sink, when an unrecoverable error happens while writing to Prometheus remote-write endpoint, is configurable.

There are 3 error conditions:

1. Prometheus returns a non-retriable error response (i.e. any `4xx` status code except `429`)
2. Prometheus returns a retriable error response (i.e. `5xx` or `429`) but the max retry limit is exceeded
3. The http client fails to complete the request, for an I/O error

The default behavior of the sink, for all these three error conditions, is to `FAIL`: throw a `PrometheusSinkWriteException`, causing the job to fail.

Optionally, for each of these error conditions independently, the sink can be configured to `DISCARD_AND_CONTINUE`: "log, discard the offending request and continue".

The error handling behaviors can be configured when creating the instance of the sink, as shown in this snipped:

```java
PrometheusSink sink =  PrometheusSink.builder()
    // ...    
    .setErrorHandlingBehaviourConfiguration(SinkWriterErrorHandlingBehaviorConfiguration.builder()
        .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
        .onMaxRetryExceeded(OnErrorBehavior.DISCARD_AND_CONTINUE)
        .onHttpClientIOFail(OnErrorBehavior.DISCARD_AND_CONTINUE)
        .build())
    .build();
```

When configured for `DISCARD_AND_CONTINUE`, the sink will do the following:

1. Log a message at `WARN` level, with information about the problem and the number of time-series and samples dropped
2. Update the counters (see below)
3. Drop the offending write-request
4. Continue with the next request

Note that there is no partial-failure condition: the entire write-request is discarded regardless what data in the request are causing the problem.
Prometheus does not return sufficient information to automatically handle partial requests.

### Metrics

The sink exposes custom metrics, counting the samples and write-requests (batches) successfully written or discarded.

* `numSamplesOut` number of samples successfully written to Prometheus
* `numWriteRequestsOut` number of write-requests successfully written to Prometheus
* `numWriteRequestsRetries` number of write requests retried due to a retriable error (e.g. throttling)
* `numSamplesDropped` number of samples dropped, for any reasons
* `numSamplesNonRetriableDropped` (when `onPrometheusNonRetriableError` is set to `DISCARD_AND_CONTINUE`) number of samples dropped due to non-retriable errors
* `numSamplesRetryLimitDropped` (when `onMaxRetryExceeded` is set to `DISCARD_AND_CONTINUE`) number of samples dropped due to reaching the max number of retries
* `numWriteRequestsPermanentlyFailed` number of write requests permanently failed, due to any reasons (non retryable, max nr of retries)

**Note**: the `numByteSend` does not measure the number of bytes, due to an internal limitation of the base sink. 
This metric should be ignored and you should rely on `numSamplesOut` and `numWriteRequestsOut` instead.

