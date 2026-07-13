# Prometheus Sink Connector - Integration Tests Documentation

## Overview

These integration tests validate the end-to-end behavior of the Prometheus Sink Connector by writing `PrometheusTimeSeries` data through the `PrometheusSink` to a **real containerized Prometheus instance** and verifying the data arrives correctly via the Prometheus Query API.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Test JVM                                      │
│                                                                      │
│  ┌──────────────┐    ┌──────────────────┐    ┌───────────────────┐  │
│  │  Test Data   │───▶│  Flink MiniCluster│───▶│  PrometheusSink   │  │
│  │  (fromColl.) │    │  (StreamExecEnv)  │    │  (Remote Write)   │  │
│  └──────────────┘    └──────────────────┘    └────────┬──────────┘  │
│                                                        │             │
│  ┌──────────────────┐                                  │             │
│  │ PrometheusQuery  │◀─── HTTP GET /api/v1/query ──────┼─────┐      │
│  │    Client        │                                  │     │      │
│  └──────────────────┘                                  │     │      │
│                                                        ▼     │      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              Docker (Testcontainers)                          │    │
│  │  ┌─────────────────────────────────────────────────────┐     │    │
│  │  │         prom/prometheus:v2.51.0                      │     │    │
│  │  │                                                      │     │    │
│  │  │  POST /api/v1/write  ◀── Remote Write Protocol ─────┘     │    │
│  │  │  GET  /api/v1/query  ──▶ PromQL Query Results  ───────────┘    │
│  │  │                                                      │     │
│  │  │  Flags: --web.enable-remote-write-receiver           │     │
│  │  └─────────────────────────────────────────────────────┘     │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### Workflow (common to all tests)

1. **Container Startup** — Testcontainers starts a Prometheus Docker container with Remote Write Receiver enabled
2. **MiniCluster Startup** — Flink MiniCluster starts with 1 TaskManager, 2 task slots
3. **Data Creation** — Test creates `PrometheusTimeSeries` objects (metric name, labels, samples)
4. **Pipeline Execution** — Flink pipeline reads from a bounded source, writes via `PrometheusSink` using the Prometheus Remote Write protocol (protobuf + snappy compressed HTTP POST)
5. **Verification** — Test polls the Prometheus Query API (`/api/v1/query`) using Awaitility until data is queryable
6. **Assertions** — Validates metric counts, values, and labels match what was written

---

## Test Class: `PrometheusSinkIT`

### Test 1: `singleTimeSeriesWrittenAndQueryable`

**What it tests:**  
Basic end-to-end write of a single time-series with one sample to Prometheus.

**Process:**
1. Creates one `PrometheusTimeSeries` with:
   - Metric name: `integration_test_single_metric`
   - Labels: `instance=test-instance-1`, `job=integration-test`
   - One sample: value `42.0` at current timestamp
2. Sends it through the Flink pipeline via `PrometheusSink`
3. Waits up to 30 seconds for Prometheus to ingest the data
4. Asserts:
   - At least 1 time-series exists for this metric
   - The sample value is `42.0`
   - The `instance` label is `test-instance-1`
   - The `job` label is `integration-test`

**Why it matters:**  
Validates the fundamental write path — a single element goes in, arrives in Prometheus with correct metric name, labels, and value. If this fails, nothing else will work.

---

### Test 2: `multipleTimeSeriesWithDifferentLabels`

**What it tests:**  
Writing multiple distinct time-series (same metric name, different label sets) and verifying Prometheus treats them as separate series.

**Process:**
1. Creates two `PrometheusTimeSeries` with the same metric name `integration_test_multi_labels` but different labels:
   - Series 1: `instance=host-a`, `region=eu-west-1`, value `10.0`
   - Series 2: `instance=host-b`, `region=us-east-1`, value `20.0`
2. Sends both through the pipeline in a single batch
3. Waits for Prometheus to show exactly 2 distinct time-series
4. Asserts:
   - Metric count is exactly 2
   - Both `host-a` and `host-b` instance labels exist

**Why it matters:**  
Prometheus identifies unique time-series by the combination of metric name + label set. This test verifies the connector correctly preserves label differentiation and doesn't merge or drop series.

---

### Test 3: `timeSeriesWithMultipleSamples`

**What it tests:**  
Writing a single time-series that contains multiple samples at different timestamps.

**Process:**
1. Creates one `PrometheusTimeSeries` with:
   - Metric name: `integration_test_multi_samples`
   - Label: `instance=sampler`
   - Three samples: `1.0` at T, `2.0` at T+1s, `3.0` at T+2s
2. Sends through the pipeline
3. Waits for the metric to appear in Prometheus
4. Asserts:
   - At least 1 time-series exists
   - The latest value is `3.0` (Prometheus instant query returns the most recent sample)

**Why it matters:**  
The Prometheus Remote Write protocol allows batching multiple samples per time-series. This validates that all samples are written and Prometheus correctly stores the time-ordered data. The `AsyncSinkBase` batching logic (which batches by sample count) is exercised here.

---

### Test 4: `highThroughputWriteFromSequenceSource`

**What it tests:**  
Writing a high volume of distinct time-series (100 elements) generated from a FLIP-27 `NumberSequenceSource`.

**Process:**
1. Uses `NumberSequenceSource(0, 99)` to generate 100 elements
2. Maps each number `i` to a unique `PrometheusTimeSeries`:
   - Metric name: `integration_test_throughput`
   - Label: `index=<i>` (unique per series)
   - Sample: value `i` at current timestamp
3. Sends all 100 through the pipeline
4. Waits up to 60 seconds for all 100 distinct series to appear
5. Asserts: metric count equals exactly 100

**Why it matters:**  
Tests the connector under realistic throughput conditions:
- Exercises the `AsyncSinkBase` batching logic (500 sample batch limit, 1s buffer timeout)
- Validates that the HTTP client handles multiple Remote Write requests
- Ensures no data loss under higher volume
- Uses the FLIP-27 Source API (unbounded-style source, even though bounded)

---

### Test 5: `sinkWithKeyByForParallelismGreaterThanOne`

**What it tests:**  
Writing with parallelism > 1 using `PrometheusTimeSeriesLabelsAndMetricNameKeySelector` to ensure correct partitioning.

**Process:**
1. Sets pipeline parallelism to 2
2. Creates three `PrometheusTimeSeries`:
   - `partition=A`, value `100.0` at T
   - `partition=B`, value `200.0` at T
   - `partition=A`, value `150.0` at T+1s (same label set as first — must go to same subtask)
3. Applies `keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector())` before the sink
4. Sends through the pipeline
5. Waits for Prometheus to show exactly 2 distinct series (A and B)
6. Asserts both partitions exist

**Why it matters:**  
Prometheus rejects out-of-order writes for the same time-series. When running with parallelism > 1, two sink subtasks could write to the same series concurrently, causing out-of-order rejections. The `keyBy` with `PrometheusTimeSeriesLabelsAndMetricNameKeySelector` ensures all samples for a given label set go to the same subtask, maintaining write order. This test validates that pattern works correctly end-to-end.

---

## Test Class: `PrometheusSinkErrorHandlingIT`

### Test 1: `shouldDiscardAndContinueOnMaxRetryExceeded`

**What it tests:**  
That the sink completes successfully (doesn't throw) when configured with `DISCARD_AND_CONTINUE` error handling behavior.

**Process:**
1. Creates a single `PrometheusTimeSeries` writing to a valid Prometheus endpoint
2. Configures the sink with:
   - `onMaxRetryExceeded = DISCARD_AND_CONTINUE`
   - `onPrometheusNonRetryableError = DISCARD_AND_CONTINUE`
3. Executes the pipeline
4. Asserts: pipeline completes without exception

**Why it matters:**  
The `DISCARD_AND_CONTINUE` behavior is critical for production resilience — operators may prefer dropping some metrics rather than failing the entire Flink job. This test validates that the error handling configuration works end-to-end and the job terminates cleanly even if some writes encounter issues.

---

## Supporting Classes

| Class | Purpose |
|-------|---------|
| `PrometheusContainer` | Testcontainers wrapper that starts `prom/prometheus:v2.51.0` with `--web.enable-remote-write-receiver` flag enabled |
| `PrometheusQueryClient` | HTTP client that queries Prometheus `/api/v1/query` endpoint to verify written data (counts, values, labels) |
| `ThroughputTestMapper` | Serializable `MapFunction` that converts `Long` sequence numbers into `PrometheusTimeSeries` objects |

## Configuration Details

| Parameter | Value | Reason |
|-----------|-------|--------|
| `maxBatchSizeInSamples` | 500 | Default production value |
| `maxTimeInBufferMS` | 1000 | Low value for fast test flushing |
| `maxRetryCount` | 5 | Enough retries for transient issues |
| `initialRetryDelayMS` | 100 | Fast retries for test speed |
| `socketTimeoutMs` | 5000 | Reasonable timeout for Docker networking |
| `onPrometheusNonRetryableError` | `DISCARD_AND_CONTINUE` | Only supported value in this connector version |
| Awaitility timeout | 30-60s | Prometheus may need time to make data queryable |
| Awaitility pollInterval | 2-3s | Balance between speed and not overwhelming Prometheus |

## Running the Tests

```bash
cd /path/to/flink-connector-prometheus
mvn clean verify -pl flink-connector-prometheus-e2e-tests -am
```

**Prerequisites:**
- Docker must be running (Testcontainers will pull `prom/prometheus:v2.51.0`)
- JDK 17+ (the project compiles to Java 8 bytecode but runs tests on JDK 17)
