/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.legacy.metrics.MetricFactory;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NanosToMillisCounter;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.legacy.metrics.QueryLatencyMetrics;

public class QueryLatencyMetricsTest {

  private final AtomicLong clock = new AtomicLong();

  @Before
  public void setUp() {
    Metrics.getInstance().clear();
    for (MetricName name :
        new MetricName[] {
          MetricName.SQL_QUERY_TIME,
          MetricName.SQL_QUERY_TIMED_COUNT,
          MetricName.PPL_QUERY_TIME,
          MetricName.PPL_QUERY_TIMED_COUNT
        }) {
      Metrics.getInstance().registerMetric(MetricFactory.createMetric(name));
    }
  }

  @Test
  public void nanosCounterReportsMillisWithoutPerQueryTruncation() {
    NanosToMillisCounter counter = new NanosToMillisCounter();
    counter.add(1_500_000L);
    counter.add(1_500_000L);
    assertThat(counter.getValue(), equalTo(3L));
  }

  @Test
  public void latencyMetricsAreNumericalAndUseNanosCounterForTime() {
    for (MetricName name :
        new MetricName[] {
          MetricName.SQL_QUERY_TIME,
          MetricName.SQL_QUERY_TIMED_COUNT,
          MetricName.PPL_QUERY_TIME,
          MetricName.PPL_QUERY_TIMED_COUNT
        }) {
      assertThat(name.isNumerical(), equalTo(true));
    }
    NumericMetric<?> time =
        (NumericMetric<?>) MetricFactory.createMetric(MetricName.SQL_QUERY_TIME);
    assertThat(time.getCounter(), instanceOf(NanosToMillisCounter.class));
  }

  @Test
  public void recordAddsElapsedTimeAndCountToStats() {
    QueryLatencyMetrics.record(
        MetricName.SQL_QUERY_TIME, MetricName.SQL_QUERY_TIMED_COUNT, 25_000_000L);
    QueryLatencyMetrics.record(
        MetricName.SQL_QUERY_TIME, MetricName.SQL_QUERY_TIMED_COUNT, 15_000_000L);

    JSONObject stats = new JSONObject(Metrics.getInstance().collectToJSON());
    assertThat(stats.getLong("sql_query_time_in_millis"), equalTo(40L));
    assertThat(stats.getLong("sql_query_timed_count"), equalTo(2L));
    assertThat(stats.getLong("ppl_query_time_in_millis"), equalTo(0L));
    assertThat(stats.getLong("ppl_query_timed_count"), equalTo(0L));
  }

  @Test
  public void recordIsNoOpWhenMetricsNotRegistered() {
    Metrics.getInstance().clear();
    QueryLatencyMetrics.record(MetricName.PPL_QUERY_TIME, MetricName.PPL_QUERY_TIMED_COUNT, 1L);
  }

  @Test
  public void listenerRecordsOnResponse() {
    @SuppressWarnings("unchecked")
    ActionListener<String> delegate = mock(ActionListener.class);
    clock.set(100L);
    ActionListener<String> timed =
        QueryLatencyMetrics.timedListener(
            delegate, MetricName.PPL_QUERY_TIME, MetricName.PPL_QUERY_TIMED_COUNT, clock::get);
    clock.set(100L + 7_000_000L);

    timed.onResponse("ok");

    verify(delegate).onResponse("ok");
    assertThat(pplTimeMillis(), equalTo(7L));
    assertThat(pplCount(), equalTo(1L));
  }

  @Test
  public void listenerDoesNotRecordOnFailure() {
    @SuppressWarnings("unchecked")
    ActionListener<String> delegate = mock(ActionListener.class);
    ActionListener<String> timed =
        QueryLatencyMetrics.timedListener(
            delegate, MetricName.PPL_QUERY_TIME, MetricName.PPL_QUERY_TIMED_COUNT, clock::get);
    Exception e = new RuntimeException("boom");

    timed.onFailure(e);

    verify(delegate).onFailure(e);
    assertThat(pplCount(), equalTo(0L));
  }

  @Test
  public void channelRecordsOnSuccessfulResponse() {
    RestChannel delegate = mock(RestChannel.class);
    RestResponse response = response(RestStatus.OK);
    clock.set(0L);
    RestChannel timed =
        QueryLatencyMetrics.timedChannel(
            delegate, MetricName.SQL_QUERY_TIME, MetricName.SQL_QUERY_TIMED_COUNT, clock::get);
    clock.set(12_000_000L);

    timed.sendResponse(response);

    verify(delegate).sendResponse(response);
    assertThat(sqlTimeMillis(), equalTo(12L));
    assertThat(sqlCount(), equalTo(1L));
  }

  @Test
  public void channelDoesNotRecordOnErrorResponse() {
    RestChannel delegate = mock(RestChannel.class);
    RestChannel timed =
        QueryLatencyMetrics.timedChannel(
            delegate, MetricName.SQL_QUERY_TIME, MetricName.SQL_QUERY_TIMED_COUNT, clock::get);

    timed.sendResponse(response(RestStatus.BAD_REQUEST));
    assertThat(sqlCount(), equalTo(0L));

    RestChannel timed2 =
        QueryLatencyMetrics.timedChannel(
            delegate, MetricName.SQL_QUERY_TIME, MetricName.SQL_QUERY_TIMED_COUNT, clock::get);
    timed2.sendResponse(response(RestStatus.INTERNAL_SERVER_ERROR));
    assertThat(sqlCount(), equalTo(0L));
  }

  @Test
  public void channelDelegatesRequest() {
    RestChannel delegate = mock(RestChannel.class);
    RestChannel timed =
        QueryLatencyMetrics.timedChannel(
            delegate, MetricName.SQL_QUERY_TIME, MetricName.SQL_QUERY_TIMED_COUNT, clock::get);
    timed.request();
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

  private static long sqlTimeMillis() {
    return value("sql_query_time_in_millis");
  }

  private static long sqlCount() {
    return value("sql_query_timed_count");
  }

  private static long pplTimeMillis() {
    return value("ppl_query_time_in_millis");
  }

  private static long pplCount() {
    return value("ppl_query_timed_count");
  }
}
