## Request Signer for Amazon Managed Prometheus (AMP)

Request signer implementation for Amazon Managed Prometheus (AMP)

Signs remote-write requests using the active IAM profile.

The Flink application requires `RemoteWrite` permissions to the AMP workspace (e.g. `AmazonPromethusRemoteWriteAccess` policy).

For an example of its usage, see the [sample application](../msf-amp-example).
