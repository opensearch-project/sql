/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.transport.TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.lucene.search.TotalHits;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.executor.ActionRequestRestExecutorFactory;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.SqlOpenSearchRequestBuilder;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/** Unit tests for PIT lifecycle management in PrettyFormatRestExecutor. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class PrettyFormatRestExecutorPitTest {

  @Mock private Client client;
  @Mock private DefaultQueryAction queryAction;
  @Mock private SqlRequest sqlRequest;
  @Mock private SqlOpenSearchRequestBuilder requestBuilder;
  @Mock private SearchRequestBuilder searchRequestBuilder;
  @Mock private SearchResponse searchResponse;
  @Mock private SearchHit searchHit;

  @Mock private RestChannel channel;

  private PrettyFormatRestExecutor executor;
  private Map<String, String> params;
  private OpenSearchSettings settings;

  @Before
  public void setUp() throws Exception {
    settings = mock(OpenSearchSettings.class);
    LocalClusterState.state().setPluginSettings(settings);

    when(queryAction.getSqlRequest()).thenReturn(sqlRequest);
    when(queryAction.explain()).thenReturn(requestBuilder);
    when(queryAction.getRequestBuilder()).thenReturn(searchRequestBuilder);
    when(searchRequestBuilder.get()).thenReturn(searchResponse);

    SearchHits hits =
        new SearchHits(
            new SearchHit[] {searchHit}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
    when(searchResponse.getHits()).thenReturn(hits);

    executor = new PrettyFormatRestExecutor("jdbc");
    params = new HashMap<>();
  }

  @Test
  public void testNoPitCreatedWhenFetchSizeIsZero() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(0);

    executor.execute(client, params, queryAction);

    verify(searchRequestBuilder, never()).setPointInTime(any(PointInTimeBuilder.class));
    verify(searchRequestBuilder, times(1)).get();
  }

  @Test
  public void testNoPitCreatedWhenFetchSizeNotSpecified() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(null); // Simulates fetch_size not in request

    executor.execute(client, params, queryAction);

    verify(searchRequestBuilder, never()).setPointInTime(any(PointInTimeBuilder.class));
  }

  @Test
  public void testFetchSizeSpecifiedButResultsFitInOnePage() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(100);

    SearchHits hits =
        new SearchHits(
            new SearchHit[] {searchHit}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
    when(searchResponse.getHits()).thenReturn(hits);

    String result = executor.execute(client, params, queryAction);

    assertNotNull("Query should execute successfully", result);
  }

  @Test
  public void testCursorCreatedWhenResultsExceedFetchSize() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(5);

    SearchHits hits =
        new SearchHits(
            new SearchHit[] {searchHit}, new TotalHits(10, TotalHits.Relation.EQUAL_TO), 1.0F);
    when(searchResponse.getHits()).thenReturn(hits);

    String result = executor.execute(client, params, queryAction);

    assertNotNull("Query should execute successfully", result);
  }

  @Test
  public void testQueryExecutionSucceedsWithVariousFetchSizes() throws Exception {
    int[] fetchSizes = {0, 100, 1};

    for (int fetchSize : fetchSizes) {
      when(sqlRequest.fetchSize()).thenReturn(fetchSize);
      String result = executor.execute(client, params, queryAction);
      assertNotNull("Result should not be null for fetchSize=" + fetchSize, result);
    }
  }

  @Test
  public void testPitCreationDeniedReturnsForbidden() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(5);
    failPitCreationWith(
        new OpenSearchSecurityException(
            "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN));

    BytesRestResponse response = executeAndCaptureResponse();

    assertEquals(RestStatus.FORBIDDEN, response.status());
    JSONObject body = new JSONObject(response.content().utf8ToString());
    assertEquals(403, body.getInt("status"));
    assertEquals("OpenSearchSecurityException", body.getJSONObject("error").getString("type"));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void testPitCreationServerErrorKeepsItsStatus() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(5);
    failPitCreationWith(
        new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE));

    BytesRestResponse response = executeAndCaptureResponse();

    assertEquals(RestStatus.SERVICE_UNAVAILABLE, response.status());
    assertEquals(503, new JSONObject(response.content().utf8ToString()).getInt("status"));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void testPitCreationRejectedReturnsTooManyRequests() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(5);
    failPitCreationWith(new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS));

    BytesRestResponse response = executeAndCaptureResponse();

    assertEquals(RestStatus.TOO_MANY_REQUESTS, response.status());
    JSONObject body = new JSONObject(response.content().utf8ToString());
    assertEquals(429, body.getInt("status"));
    assertEquals("OpenSearchStatusException", body.getJSONObject("error").getString("type"));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void testBadRequestInJdbcFormatCountsAsClientFailureOnce() throws Exception {
    registerFailureMetrics();
    when(sqlRequest.fetchSize()).thenReturn(0);
    when(searchRequestBuilder.get())
        .thenThrow(new OpenSearchStatusException("bad request", RestStatus.BAD_REQUEST));

    BytesRestResponse response = executeAndCaptureResponse();

    assertEquals(RestStatus.BAD_REQUEST, response.status());
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void testFailureThatTripsTheMemoryCheckIsLeftToTheCallerToCount() throws Exception {
    registerFailureMetrics();
    when(sqlRequest.fetchSize()).thenReturn(0);
    when(searchRequestBuilder.get())
        .thenThrow(new OpenSearchStatusException("bad request", RestStatus.BAD_REQUEST));

    try (MockedStatic<BackOffRetryStrategy> backOffRetryStrategy =
        Mockito.mockStatic(BackOffRetryStrategy.class)) {
      backOffRetryStrategy
          .when(() -> BackOffRetryStrategy.isHealthy(anyLong(), any()))
          .thenReturn(false);
      assertThrows(
          IllegalStateException.class,
          () -> executor.execute(client, params, queryAction, channel));
    }

    verify(channel, never()).sendResponse(any());
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void testBadRequestInsideAsyncExecutorIsCountedOnce() throws Exception {
    registerFailureMetrics();
    when(sqlRequest.fetchSize()).thenReturn(0);
    when(searchRequestBuilder.get())
        .thenThrow(new OpenSearchStatusException("bad request", RestStatus.BAD_REQUEST));

    BytesRestResponse response = executeInWorkerThreadAndCaptureResponse(true);

    assertEquals(RestStatus.BAD_REQUEST, response.status());
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void testMemoryCheckFailureInsideAsyncExecutorIsCountedOnceAsSystemFailure()
      throws Exception {
    registerFailureMetrics();
    when(sqlRequest.fetchSize()).thenReturn(0);
    when(searchRequestBuilder.get())
        .thenThrow(new OpenSearchStatusException("bad request", RestStatus.BAD_REQUEST));

    BytesRestResponse response = executeInWorkerThreadAndCaptureResponse(false);

    assertEquals(RestStatus.INSUFFICIENT_STORAGE, response.status());
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  private void registerFailureMetrics() {
    Metrics.getInstance().clear();
    for (MetricName name :
        new MetricName[] {MetricName.FAILED_REQ_COUNT_CUS, MetricName.FAILED_REQ_COUNT_SYS}) {
      Metrics.getInstance().registerMetric(new NumericMetric<>(name.getName(), new BasicCounter()));
    }
  }

  /**
   * Runs a jdbc executor inside the async executor on a transport thread, so the query runs on the
   * worker pool, which this test runs inline.
   */
  private BytesRestResponse executeInWorkerThreadAndCaptureResponse(boolean healthy)
      throws Exception {
    when(settings.getSettingValue(Settings.Key.SQL_SLOWLOG)).thenReturn(Integer.MAX_VALUE);
    ThreadPool threadPool = mock(ThreadPool.class);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArgument(0)).run();
              return null;
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), anyString());
    when(client.threadPool()).thenReturn(threadPool);

    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName(TRANSPORT_WORKER_THREAD_NAME_PREFIX);
    try (MockedStatic<BackOffRetryStrategy> backOffRetryStrategy =
        Mockito.mockStatic(BackOffRetryStrategy.class)) {
      backOffRetryStrategy
          .when(() -> BackOffRetryStrategy.isHealthy(anyLong(), any()))
          .thenReturn(healthy);
      ActionRequestRestExecutorFactory.createExecutor(Format.JDBC)
          .execute(client, params, queryAction, channel);
    } finally {
      Thread.currentThread().setName(originalName);
    }
    ArgumentCaptor<BytesRestResponse> captor = ArgumentCaptor.forClass(BytesRestResponse.class);
    verify(channel).sendResponse(captor.capture());
    return captor.getValue();
  }

  private void failPitCreationWith(Exception failure) throws Exception {
    registerFailureMetrics();
    Select select = mock(Select.class);
    when(select.getIndexArr()).thenReturn(new String[] {"logs"});
    when(queryAction.getSelect()).thenReturn(select);
    when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    ActionFuture<CreatePitResponse> future = mock(ActionFuture.class);
    when(future.get()).thenThrow(new ExecutionException(failure));
    when(client.execute(any(CreatePitAction.class), any(CreatePitRequest.class)))
        .thenReturn(future);
  }

  private BytesRestResponse executeAndCaptureResponse() {
    // The memory check before sending reads the live heap, so it is stubbed to keep the test
    // independent of the JVM state.
    try (MockedStatic<BackOffRetryStrategy> backOffRetryStrategy =
        Mockito.mockStatic(BackOffRetryStrategy.class)) {
      backOffRetryStrategy
          .when(() -> BackOffRetryStrategy.isHealthy(anyLong(), any()))
          .thenReturn(true);
      executor.execute(client, params, queryAction, channel);
    }
    ArgumentCaptor<BytesRestResponse> captor = ArgumentCaptor.forClass(BytesRestResponse.class);
    verify(channel).sendResponse(captor.capture());
    return captor.getValue();
  }

  private static long failedRequestCount(MetricName name) {
    return (Long) Metrics.getInstance().getNumericalMetric(name).getValue();
  }
}
