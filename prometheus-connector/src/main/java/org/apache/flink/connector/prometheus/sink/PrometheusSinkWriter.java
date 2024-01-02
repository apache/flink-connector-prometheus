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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.prometheus.sink.http.RemoteWriteResponseClassifier;
import org.apache.flink.connector.prometheus.sink.prometheus.Remote;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.io.CloseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_NON_RETRIABLE_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_OUT;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_SAMPLES_RETRY_LIMIT_DROPPED;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_WRITE_REQUESTS_OUT;
import static org.apache.flink.connector.prometheus.sink.SinkCounters.SinkCounter.NUM_WRITE_REQUESTS_PERMANENTLY_FAILED;

/** Writer, taking care of batching the {@link PrometheusTimeSeries} and handling retries. */
public class PrometheusSinkWriter extends AsyncSinkWriter<PrometheusTimeSeries, Types.TimeSeries> {

    /**
     * * Batching of this sink is in terms of Samples, not bytes. The goal is adaptively increase
     * the number of Samples in each batch, a WriteRequest sent to Prometheus, to a configurable
     * number. This is the parameter maxBatchSizeInBytes.
     *
     * <p>getSizeInBytes(requestEntry) returns the number of Samples (not bytes) and
     * maxBatchSizeInBytes is actually in terms of Samples (not bytes).
     *
     * <p>In AsyncSinkWriter, maxBatchSize is in terms of requestEntries (TimeSeries). But because
     * each TimeSeries contains 1+ Samples, we set maxBatchSize = maxBatchSizeInBytes.
     *
     * <p>maxRecordSizeInBytes is also calculated in the same unit assumed by getSizeInBytes(..). In
     * our case is the max number of Samples in a single TimeSeries sent to the Sink. We are
     * limiting the number of Samples in each TimeSeries to the max batch size, setting
     * maxRecordSizeInBytes = maxBatchSizeInBytes.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkWriter.class);

    private final SinkCounters counters;
    private final CloseableHttpAsyncClient asyncHttpClient;
    private final PrometheusRemoteWriteHttpRequestBuilder requestBuilder;

    public PrometheusSinkWriter(
            ElementConverter<PrometheusTimeSeries, Types.TimeSeries> elementConverter,
            Sink.InitContext context,
            int maxInFlightRequests,
            int maxBufferedRequests,
            int maxBatchSizeInSamples,
            long maxTimeInBufferMS,
            String prometheusRemoteWriteUrl,
            CloseableHttpAsyncClient asyncHttpClient,
            SinkCounters counters,
            PrometheusRequestSigner requestSigner) {
        this(
                elementConverter,
                context,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInSamples,
                maxTimeInBufferMS,
                prometheusRemoteWriteUrl,
                asyncHttpClient,
                counters,
                requestSigner,
                Collections.emptyList());
    }

    public PrometheusSinkWriter(
            ElementConverter<PrometheusTimeSeries, Types.TimeSeries> elementConverter,
            Sink.InitContext context,
            int maxInFlightRequests,
            int maxBufferedRequests,
            int maxBatchSizeInSamples,
            long maxTimeInBufferMS,
            String prometheusRemoteWriteUrl,
            CloseableHttpAsyncClient asyncHttpClient,
            SinkCounters counters,
            PrometheusRequestSigner requestSigner,
            Collection<BufferedRequestState<Types.TimeSeries>> states) {
        super(
                elementConverter,
                context,
                maxBatchSizeInSamples, // maxBatchSize
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInSamples, // maxBatchSizeInBytes
                maxTimeInBufferMS,
                maxBatchSizeInSamples, // maxRecordSizeInBytes
                states);
        this.requestBuilder =
                new PrometheusRemoteWriteHttpRequestBuilder(
                        prometheusRemoteWriteUrl, requestSigner);
        this.asyncHttpClient = asyncHttpClient;
        this.counters = counters;
    }

    /**
     * This is the "size" of the request entry (a {@link Types.TimeSeries time-series}) used for
     * batching. Regardless the name of the method, it returns the number of {@link Types.Sample
     * samples} in the time-series (not bytes), to support batching in terms of samples.
     *
     * @param timeSeries the request entry for which we want to know the size
     * @return number of samples in the time-series
     */
    @Override
    protected long getSizeInBytes(Types.TimeSeries timeSeries) {
        return RequestEntrySizeUtils.requestSizeForBatching(timeSeries);
    }

    @Override
    protected void submitRequestEntries(
            List<Types.TimeSeries> requestEntries, Consumer<List<Types.TimeSeries>> requestResult) {
        int timeSeriesCount = requestEntries.size();
        long sampleCount = RequestEntrySizeUtils.countSamples(requestEntries);
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Writing {} time-series containing {} samples ", timeSeriesCount, sampleCount);
        }

        Remote.WriteRequest writeRequest = buildWriteRequest(requestEntries);
        byte[] requestBody;
        try {
            requestBody = compressWriteRequest(writeRequest);
        } catch (IOException e) {
            throw new RuntimeException("Exception compressing the request body", e);
        }

        SimpleHttpRequest postRequest = requestBuilder.buildHttpRequest(requestBody);
        asyncHttpClient.execute(
                postRequest,
                new ResponseCallback(timeSeriesCount, sampleCount, counters, requestResult));
    }

    @VisibleForTesting
    static class ResponseCallback implements FutureCallback<SimpleHttpResponse> {
        private final int timeSeriesCount;
        private final long sampleCount;
        private final Consumer<List<Types.TimeSeries>> requestResult;
        private final SinkCounters counters;

        public ResponseCallback(
                int timeSeriesCount,
                long sampleCount,
                SinkCounters counters,
                Consumer<List<Types.TimeSeries>> requestResult) {
            this.timeSeriesCount = timeSeriesCount;
            this.sampleCount = sampleCount;
            this.requestResult = requestResult;
            this.counters = counters;
        }

        @Override
        public void completed(SimpleHttpResponse response) {
            if (RemoteWriteResponseClassifier.isSuccessResponse(response)) {
                LOG.debug(
                        "{},{} - successfully posted {} time-series, containing {} samples",
                        response.getCode(),
                        response.getReasonPhrase(),
                        timeSeriesCount,
                        sampleCount);
                counters.inc(NUM_SAMPLES_OUT, sampleCount);
                counters.inc(NUM_WRITE_REQUESTS_OUT);
            } else {
                counters.inc(NUM_SAMPLES_DROPPED, sampleCount);
                counters.inc(NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);

                String responseBody = response.getBodyText();
                if (RemoteWriteResponseClassifier.isNonRetriableErrorResponse(response)) {
                    LOG.warn(
                            "{},{} {} (discarded {} time-series, containing {} samples)",
                            response.getCode(),
                            response.getReasonPhrase(),
                            responseBody,
                            timeSeriesCount,
                            sampleCount);
                    counters.inc(NUM_SAMPLES_NON_RETRIABLE_DROPPED, sampleCount);
                } else if (RemoteWriteResponseClassifier.isRetriableErrorResponse(response)) {
                    LOG.warn(
                            "{},{} {} (after retry limit reached, discarded {} time-series, containing {} samples)",
                            response.getCode(),
                            response.getReasonPhrase(),
                            responseBody,
                            timeSeriesCount,
                            sampleCount);
                    counters.inc(NUM_SAMPLES_RETRY_LIMIT_DROPPED, sampleCount);
                }
            }

            // Never re-queue requests
            requestResult.accept(Collections.emptyList());
        }

        @Override
        public void failed(Exception ex) {
            LOG.warn(
                    "Exception executing the remote-write (discarded {} time-series containing {} samples)",
                    timeSeriesCount,
                    sampleCount,
                    ex);
            counters.inc(NUM_SAMPLES_DROPPED, sampleCount);
            counters.inc(NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);
        }

        @Override
        public void cancelled() {
            LOG.warn(
                    "Write request execution cancelled (discarded {} time-series containing {} samples)",
                    timeSeriesCount,
                    sampleCount);
            counters.inc(NUM_SAMPLES_DROPPED, sampleCount);
            counters.inc(NUM_WRITE_REQUESTS_PERMANENTLY_FAILED);
        }
    }

    @Override
    public void close() {
        if (this.asyncHttpClient != null) {
            this.asyncHttpClient.close(CloseMode.GRACEFUL);
        }
        super.close();
    }

    private Remote.WriteRequest buildWriteRequest(List<Types.TimeSeries> requestEntries) {
        var builder = Remote.WriteRequest.newBuilder();
        for (Types.TimeSeries timeSeries : requestEntries) {
            builder.addTimeseries(timeSeries);
        }
        return builder.build();
    }

    private byte[] compressWriteRequest(Remote.WriteRequest writeRequest) throws IOException {
        return Snappy.compress(writeRequest.toByteArray());
    }
}
