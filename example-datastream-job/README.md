## Example  job using Prometheus Sink connector with DataStream API

Sample application demonstrating the usage of Prometheus Sink Connector with DataStream API.

The example demonstrates how to write to a generic, unauthenticated Prometheus remote-write URL, and optionally how to use the Amazon Managed Prometheus request signer.

It generates random dummy Memory and CPU metrics from a number of instances, and writes them to Prometheus.

### Configuration

The application expects these parameters, via command line:

* `--prometheusRemoteWriteUrl <URL>`: the Prometheus remote-write URL to target
* `--awsRegion <region>`: (optional) if specified, it configures the Amazon Managed Prometheus request signer for a workspace in this Region
* `--webUI`: (optional, for local development only) enables Flink Web UI, with flame graphs, for local development

### Data generation

The application generates random time series, containing `CPU` and `Memory` samples from 5 dummy "instances". A new time series is generated about every 100ms. Each time series contains 1 to 10 samples.

These parameters are configurable from the code

