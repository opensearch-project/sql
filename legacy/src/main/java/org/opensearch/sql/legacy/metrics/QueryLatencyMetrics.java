/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import java.io.IOException;
import java.util.function.LongSupplier;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;

/**
 * Records end-to-end query latency into a cumulative (total time, count) metric pair. Consumers
 * derive average latency over an interval as delta(time) / delta(count), the same shape as core's
 * {@code query_time_in_millis} / {@code query_total}. Only successful queries are recorded.
 */
public final class QueryLatencyMetrics {

  private QueryLatencyMetrics() {}

  /** Add one timed query to the given metric pair. No-op if the metrics are not registered. */
  public static void record(MetricName timeMetric, MetricName countMetric, long elapsedNanos) {
    NumericMetric<?> time = Metrics.getInstance().getNumericalMetric(timeMetric);
    NumericMetric<?> count = Metrics.getInstance().getNumericalMetric(countMetric);
    if (time == null || count == null) {
      return;
    }
    time.increment(Math.max(0L, elapsedNanos));
    count.increment();
  }

  /** Wrap a listener so a successful response records the time since this call. */
  public static <T> ActionListener<T> timedListener(
      ActionListener<T> delegate, MetricName timeMetric, MetricName countMetric) {
    return timedListener(delegate, timeMetric, countMetric, System::nanoTime);
  }

  public static <T> ActionListener<T> timedListener(
      ActionListener<T> delegate,
      MetricName timeMetric,
      MetricName countMetric,
      LongSupplier nanoClock) {
    final long start = nanoClock.getAsLong();
    return new ActionListener<>() {
      @Override
      public void onResponse(T response) {
        try {
          record(timeMetric, countMetric, nanoClock.getAsLong() - start);
        } finally {
          delegate.onResponse(response);
        }
      }

      @Override
      public void onFailure(Exception e) {
        delegate.onFailure(e);
      }
    };
  }

  /** Wrap a REST channel so a 2xx response records the time since this call. */
  public static RestChannel timedChannel(
      RestChannel delegate, MetricName timeMetric, MetricName countMetric) {
    return timedChannel(delegate, timeMetric, countMetric, System::nanoTime);
  }

  public static RestChannel timedChannel(
      RestChannel delegate, MetricName timeMetric, MetricName countMetric, LongSupplier nanoClock) {
    return new TimedRestChannel(delegate, timeMetric, countMetric, nanoClock);
  }

  private static final class TimedRestChannel implements RestChannel {
    private final RestChannel delegate;
    private final MetricName timeMetric;
    private final MetricName countMetric;
    private final LongSupplier nanoClock;
    private final long start;

    TimedRestChannel(
        RestChannel delegate,
        MetricName timeMetric,
        MetricName countMetric,
        LongSupplier nanoClock) {
      this.delegate = delegate;
      this.timeMetric = timeMetric;
      this.countMetric = countMetric;
      this.nanoClock = nanoClock;
      this.start = nanoClock.getAsLong();
    }

    @Override
    public void sendResponse(RestResponse response) {
      try {
        int status = response.status().getStatus();
        if (status >= 200 && status < 300) {
          record(timeMetric, countMetric, nanoClock.getAsLong() - start);
        }
      } finally {
        delegate.sendResponse(response);
      }
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
      return delegate.newBuilder();
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
      return delegate.newErrorBuilder();
    }

    @Override
    public XContentBuilder newBuilder(MediaType mediaType, boolean useFiltering)
        throws IOException {
      return delegate.newBuilder(mediaType, useFiltering);
    }

    @Override
    public XContentBuilder newBuilder(
        MediaType mediaType, MediaType responseContentType, boolean useFiltering)
        throws IOException {
      return delegate.newBuilder(mediaType, responseContentType, useFiltering);
    }

    @Override
    public BytesStreamOutput bytesOutput() {
      return delegate.bytesOutput();
    }

    @Override
    public RestRequest request() {
      return delegate.request();
    }

    @Override
    public boolean detailedErrorsEnabled() {
      return delegate.detailedErrorsEnabled();
    }

    @Override
    public boolean detailedErrorStackTraceEnabled() {
      return delegate.detailedErrorStackTraceEnabled();
    }
  }
}
