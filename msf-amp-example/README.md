## Sample application: Amazon Managed Service for Apache Flink and Amazon Managed Prometheus

Sample application demonstrating the usage of Flink Prometheus connector writing to Amazon Managed Prometheus (AMP) with request signing. 

The application generates random dummy Memory and CPU metrics from a number of instances, and writes them to Prometheus.

The application can run locally and on Amazon Managed Service for Apache Flink.

Data generation and sink parallelism be different, to allow running some comparative performance testing.

### Authentication

The application signs the requests to AMP using the active IAM profile.

It requires RemoteWrite permissions to the AMP workspace (e.g. `AmazonPromethusRemoteWriteAccess` policy)

### Application config

* `Region <aws-region>`
* `PrometheusWriteUrl <remote-write-url` e.g. `https://aps-workspaces.eu-west-1.amazonaws.com/workspaces/ws-abcdef-123456789/api/v1/remote_write
* `SinkParallelismDivisor <div>` (optional) if provided, the sink operator will have a parallelism equal to the application parallelism divided by this value. By default, the sink will have the application parallelism.
* Sink configurations (all optional)
    * `MaxRequestRetryCount <count>` max number of retries, on retriable errors (default: 2^31-1 ~ infinite)
* Data generator configuration (all optional)
    * `GeneratorMinNrOfSamplesPerTimeSeries <min-samples-per-time-series>` min number of Samples per generated Time-Series (default = 2)
    * `GeneratorMaxNrOfSamplesPerTimeSeries <max-samples-per-time-series>` min number of Samples per generated Time-Series (default = 10)
    * `GeneratorNumberOfDummySources <dummy-sources>` number of dummy source "instances" generating metrics (default = 60) - **MUST be a multiple of generator parallelism**
    * `GeneratorPauseBetweenTimeSeriesMs <pause-millis>` pause between each time-series, in milliseconds (default=100)

All parameters are case-sensitive.

> **Attention**: if `GeneratorNumberOfDummySources` is not a multiple of the generator parallelism, the application does not start.

> The cardinality of time-series is 2x `GeneratorNumberOfDummySources`. Two metrics are generated for each dummy source.

### Parallelism

* data generator operator: application parallelism
* sink operator: application-parallelism / `SinkParallelismDivisor` (default = 1)

### Running locally

The application runs with parallelism 2 and operator chaining disabled (the data generator is always par=1)

Configuration parameters passed via command line, prepending `--`, e.g. `--Region eu-west-1`

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

Data are generated in parallel, with the application parallelism.
**The number of dummy data sources (Instanced) must be divisible by the parallelism.**
