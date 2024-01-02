## Sample application: Amazon Managed Service for Apache Flink and Amazon Managed Prometheus

Sample application demonstrating the usage of Flink Prometheus connector writing to Amazon Managed Prometheus (AMP) with request signing. 

The application generates random dummy Memory and CPU metrics from a number of instances, and writes them to Prometheus.

The application can run locally and on Amazon Managed Service for Apache Flink.

The sink can be tested with any parallelism (= application parallelism).

### Authentication

The application signs the requests to AMP using the active IAM profile.

It requires RemoteWrite permissions to the AMP workspace (e.g. `AmazonPromethusRemoteWriteAccess` policy)

### Application config

* `Region <aws-region>`
* `PrometheusWriteUrl <remote-write-url` e.g. `https://aps-workspaces.eu-west-1.amazonaws.com/workspaces/ws-abcdef-123456789/api/v1/remote_write
* Sink configurations (all optional)
    * `MaxRequestRetryCount <count>` max number of retries, on retriable errors (default: 2^31-1 ~ infinite)
* Data generator configuration (all optional)
    * `GeneratorMinNrOfSamplesPerTimeSeries <min-samples-per-time-series>` min number of Samples per generated Time-Series (default = 2)
    * `GeneratorMaxNrOfSamplesPerTimeSeries <max-samples-per-time-series>` min number of Samples per generated Time-Series (default = 10)
    * `GeneratorNumberOfDummyInstances <nr-of-instances>` number of dummy source "instances", each generating "CPU" and "Memory" metrics (default = 60)
    * `GeneratorPauseBetweenTimeSeriesMs <pause-millis>` pause between each generated time-series, in milliseconds (default=100)

All parameters are case-sensitive.

> The cardinality of time-series is 2x `GeneratorNumberOfDummyInstances`. Two metrics (`CPU` and `Memory`) are generated for each dummy source.

### Sink Parallelism

The sink can run with any parallelism, the application parallelism.

Time series are generated in timestamp order by the generator. But to guarantee they are also written to Prometheus in order,
before getting to the sink the stream is keyed to ensure the same input time series (records with identical sets of labels )
are written by the same sink sub-task.

### Running locally

The application runs with parallelism 2 and operator chaining disabled (the data generator is always par=1)

Configuration parameters passed via command line, prepending `--` and separating parameter and value with a space, e.g. `--Region eu-west-1`

The additional `--WebUI` command line parameter is supported, enabling Flink UI (with FlameGraphs) on http://localhost:8081

Running in IntelliJ:

1. AWS Toolkit plugin: select profile and region
2. Run configuration
  * enable _Add dependencies with "provided" scope to classpath_
  * AWS Connection: _Use currently selected credential profile/region_

### Deploying on Amazon Managed Service for Apache Flink

The project builds the fat-jar.

Application property group: `FlinkApplicationProperties`

### Data generations

The application generates time-series at a fixed pace, adding a random number of samples (2..10 by default) to each time-series.

Samples represent `CPU` and `Memory` metrics for a random number of "instances".
The sample timestamp is based on the system time, while the value is simply random between 0 and 1.

