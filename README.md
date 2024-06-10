# Apache Flink Prometheus Connector

This repository contains the official Apache Flink Prometheus connector.

* [More details](flink-connector-prometheus/README.md) about the connector and its usage.
* [Example application](flink-connector-prometheus/src/test/java/org/apache/flink/connector/prometheus/sink/examples/DataStreamExample.java)
  demonstrating the usage of the connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Modules

This repository contains the following modules

* [Prometheus Connector](./flink-connector-prometheus): Flink Prometheus Connector implementation; supports optional
  request signer
* [Sample application](./example-datastream-job): Sample application showing the usage of the connector with DataStream
  API. It also demonstrates how to configure the request signer.
* [Amazon Managed Prometheus Request Signer](./flink-connector-prometheus-request-signer-amp): Implementation of request
  signer for Amazon Managed Prometheus (AMP)

## Building the Apache Flink Prometheus Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 11

```
git clone https://github.com/apache/flink-connector-prometheus.git
cd flink-connector-prometheus
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective module.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:

* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out
our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea)
guide for details.

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if
you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article
describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
