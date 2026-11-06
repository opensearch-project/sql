/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.LatencyMetric;
import org.opensearch.sql.legacy.metrics.LatencyMetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.QueryLatencyMetrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

public class QueryLatencyMetricsTest {

  private final AtomicLong clock = new AtomicLong();

  @Before
  public void setUp() {
    Metrics.getInstance().clear();
    for (LatencyMetricName name : LatencyMetricName.values()) {
      Metrics.getInstance().registerMetric(new LatencyMetric(name));
    }
  }

  @Test
  public void latencyMetricKeysAreStable() {
    assertThat(LatencyMetricName.SQL.getTimeKey(), equalTo("sql_query_time_in_millis"));
    assertThat(LatencyMetricName.SQL.getCountKey(), equalTo("sql_query_timed_count"));
    assertThat(LatencyMetricName.PPL.getTimeKey(), equalTo("ppl_query_time_in_millis"));
    assertThat(LatencyMetricName.PPL.getCountKey(), equalTo("ppl_query_timed_count"));
  }

  @Test
  public void sumsNanosSoSubMillisecondQueriesAreNotTruncated() {
    LatencyMetric metric = new LatencyMetric(LatencyMetricName.SQL);
    metric.record(1_500_000L);
    metric.record(1_500_000L);
    assertThat(metric.getValue().getTotalMillis(), equalTo(3L));
    assertThat(metric.getValue().getCount(), equalTo(2L));
  }

  @Test
  public void negativeElapsedIsClampedToZero() {
    LatencyMetric metric = new LatencyMetric(LatencyMetricName.SQL);
    metric.record(-5L);
    assertThat(metric.getValue().getTotalNanos(), equalTo(0L));
    assertThat(metric.getValue().getCount(), equalTo(1L));
  }

  @Test
  public void registerDefaultMetricsIncludesLatencyKeys() {
    // Rolling counters in the default set read their window settings at construction.
    OpenSearchSettings settings = mock(OpenSearchSettings.class);
    when(settings.getSettingValue(Settings.Key.METRICS_ROLLING_INTERVAL)).thenReturn(60L);
    when(settings.getSettingValue(Settings.Key.METRICS_ROLLING_WINDOW)).thenReturn(3600L);
    LocalClusterState.state().setPluginSettings(settings);
    Metrics.getInstance().clear();
    Metrics.getInstance().registerDefaultMetrics();
    JSONObject stats = new JSONObject(Metrics.getInstance().collectToJSON());
    for (LatencyMetricName name : LatencyMetricName.values()) {
      assertThat(stats.getLong(name.getTimeKey()), equalTo(0L));
      assertThat(stats.getLong(name.getCountKey()), equalTo(0L));
    }
  }

  @Test
  public void recordAddsElapsedTimeAndCountToStats() {
    QueryLatencyMetrics.record(LatencyMetricName.SQL, 25_000_000L);
    QueryLatencyMetrics.record(LatencyMetricName.SQL, 15_000_000L);

    JSONObject stats = new JSONObject(Metrics.getInstance().collectToJSON());
    assertThat(stats.getLong("sql_query_time_in_millis"), equalTo(40L));
    assertThat(stats.getLong("sql_query_timed_count"), equalTo(2L));
    assertThat(stats.getLong("ppl_query_time_in_millis"), equalTo(0L));
    assertThat(stats.getLong("ppl_query_timed_count"), equalTo(0L));
  }

  @Test
  public void recordIsNoOpWhenMetricsNotRegistered() {
    Metrics.getInstance().clear();
    QueryLatencyMetrics.record(LatencyMetricName.PPL, 1L);
  }

  /** Every snapshot must pair a time with exactly the count that produced it. */
  @Test
  public void snapshotsAreConsistentUnderConcurrentWrites() throws Exception {
    LatencyMetric metric = new LatencyMetric(LatencyMetricName.PPL);
    long perQueryNanos = 1_000L;
    AtomicBoolean done = new AtomicBoolean();
    AtomicReference<String> violation = new AtomicReference<>();

    List<Thread> writers = new ArrayList<>();
    for (int t = 0; t < 4; t++) {
      Thread w =
          new Thread(
              () -> {
                for (int i = 0; i < 50_000; i++) {
                  metric.record(perQueryNanos);
                }
              });
      writers.add(w);
      w.start();
    }
    Thread reader =
        new Thread(
            () -> {
              while (!done.get()) {
                LatencyMetric.Snapshot s = metric.getValue();
                if (s.getTotalNanos() != s.getCount() * perQueryNanos) {
                  violation.compareAndSet(null, s.getTotalNanos() + " ns vs " + s.getCount());
                }
              }
            });
    reader.start();
    for (Thread w : writers) {
      w.join();
    }
    done.set(true);
    reader.join();

    assertThat(violation.get(), equalTo(null));
    assertThat(metric.getValue().getCount(), equalTo(200_000L));
  }

  @Test
  public void listenerRecordsOnResponse() {
    @SuppressWarnings("unchecked")
    ActionListener<String> delegate = mock(ActionListener.class);
    clock.set(100L);
    ActionListener<String> timed =
        QueryLatencyMetrics.timedListener(delegate, LatencyMetricName.PPL, clock::get);
    clock.set(100L + 7_000_000L);

    timed.onResponse("ok");

    verify(delegate).onResponse("ok");
    assertThat(value("ppl_query_time_in_millis"), equalTo(7L));
    assertThat(value("ppl_query_timed_count"), equalTo(1L));
  }

  @Test
  public void listenerDoesNotRecordOnFailure() {
    @SuppressWarnings("unchecked")
    ActionListener<String> delegate = mock(ActionListener.class);
    ActionListener<String> timed =
        QueryLatencyMetrics.timedListener(delegate, LatencyMetricName.PPL, clock::get);
    Exception e = new RuntimeException("boom");

    timed.onFailure(e);

    verify(delegate).onFailure(e);
    assertThat(value("ppl_query_timed_count"), equalTo(0L));
  }

  @Test
  public void channelRecordsOnSuccessfulResponse() {
    RestChannel delegate = mock(RestChannel.class);
    RestResponse response = response(RestStatus.OK);
    clock.set(0L);
    RestChannel timed =
        QueryLatencyMetrics.timedChannel(delegate, LatencyMetricName.SQL, clock::get);
    clock.set(12_000_000L);

    timed.sendResponse(response);

    verify(delegate).sendResponse(response);
    assertThat(value("sql_query_time_in_millis"), equalTo(12L));
    assertThat(value("sql_query_timed_count"), equalTo(1L));
  }

  @Test
  public void channelDoesNotRecordOnErrorResponse() {
    RestChannel delegate = mock(RestChannel.class);
    QueryLatencyMetrics.timedChannel(delegate, LatencyMetricName.SQL, clock::get)
        .sendResponse(response(RestStatus.BAD_REQUEST));
    QueryLatencyMetrics.timedChannel(delegate, LatencyMetricName.SQL, clock::get)
        .sendResponse(response(RestStatus.INTERNAL_SERVER_ERROR));
    assertThat(value("sql_query_timed_count"), equalTo(0L));
  }

  @Test
  public void channelDelegatesRequest() {
    RestChannel delegate = mock(RestChannel.class);
    QueryLatencyMetrics.timedChannel(delegate, LatencyMetricName.SQL, clock::get).request();
    verify(delegate).request();
  }

  private static RestResponse response(RestStatus status) {
    RestResponse response = mock(RestResponse.class);
    when(response.status()).thenReturn(status);
    return response;
  }

  private static long value(String key) {
    return new JSONObject(Metrics.getInstance().collectToJSON()).getLong(key);
  }
}
