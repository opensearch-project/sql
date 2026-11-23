/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.transport.TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.legacy.util.LogCapture;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/** Test AsyncRestExecutor behavior. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class AsyncRestExecutorTest {

  private static final boolean NON_BLOCKING = false;

  @Mock private RestExecutor executor;

  @Mock private Client client;

  private final Map<String, String> params = emptyMap();

  @Mock private QueryAction action;

  @Mock private RestChannel channel;

  @Mock private ClusterSettings clusterSettings;

  @Before
  public void setUp() {
    when(client.threadPool()).thenReturn(mock(ThreadPool.class));
    when(action.getSqlRequest()).thenReturn(SqlRequest.NULL);
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);

    OpenSearchSettings settings = spy(new OpenSearchSettings(clusterSettings));
    doReturn(emptyList()).when(settings).getSettings();
    LocalClusterState.state().setPluginSettings(settings);
  }

  @Test
  public void executeBlockingQuery() throws Exception {
    Thread.currentThread().setName(TRANSPORT_WORKER_THREAD_NAME_PREFIX);
    execute();
    verifyRunInWorkerThread();
  }

  @Test
  public void executeBlockingQueryButNotInTransport() throws Exception {
    execute();
    verifyRunInCurrentThread();
  }

  @Test
  public void executeNonBlockingQuery() throws Exception {
    execute(anyAction -> NON_BLOCKING);
    verifyRunInCurrentThread();
  }

  private void execute() throws Exception {
    AsyncRestExecutor asyncExecutor = new AsyncRestExecutor(executor);
    asyncExecutor.execute(client, params, action, channel);
  }

  private void execute(Predicate<QueryAction> isBlocking) throws Exception {
    AsyncRestExecutor asyncExecutor = new AsyncRestExecutor(executor, isBlocking);
    asyncExecutor.execute(client, params, action, channel);
  }

  private void verifyRunInCurrentThread() {
    verify(client, never()).threadPool();
  }

  private void verifyRunInWorkerThread() {
    verify(client, times(1)).threadPool();
  }

  @Test
  public void asyncClientErrorKeepsItsStatusAndCountsAsClientError() throws Exception {
    BytesRestResponse response =
        executeAsyncFailingWith(
            new OpenSearchSecurityException(
                "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN));

    assertEquals(RestStatus.FORBIDDEN, response.status());
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void asyncWrappedClientErrorIsNotReportedAsMemoryCircuitBreak() throws Exception {
    BytesRestResponse response =
        executeAsyncFailingWith(
            new IllegalStateException(
                "Error occurred during join query run",
                new OpenSearchSecurityException(
                    "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN)));

    assertEquals(RestStatus.FORBIDDEN, response.status());
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void asyncServerErrorKeepsItsStatus() throws Exception {
    BytesRestResponse response =
        executeAsyncFailingWith(
            new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE));

    assertEquals(RestStatus.SERVICE_UNAVAILABLE, response.status());
    assertEquals(503, new JSONObject(response.content().utf8ToString()).getInt("status"));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void asyncWrappedTooManyRequestsKeepsItsStatusAndCountsAsClientErrorOnce()
      throws Exception {
    BytesRestResponse response = executeAsyncFailingWith(wrappedTooManyRequests());

    assertEquals(RestStatus.TOO_MANY_REQUESTS, response.status());
    JSONObject body = new JSONObject(response.content().utf8ToString());
    assertEquals(429, body.getInt("status"));
    assertEquals("OpenSearchStatusException", body.getJSONObject("error").getString("type"));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void asyncWrappedTooManyRequestsIsLoggedAtWarnWithoutStackTrace() throws Exception {
    List<LogEvent> events =
        LogCapture.capture(
                AsyncRestExecutor.class, () -> executeAsyncFailingWith(wrappedTooManyRequests()))
            .stream()
            .filter(event -> event.getLevel().isMoreSpecificThan(Level.WARN))
            .collect(Collectors.toList());

    assertEquals(events.toString(), 1, events.size());
    assertEquals(Level.WARN, events.get(0).getLevel());
    assertNull(events.get(0).getThrown());
    assertTrue(
        events.get(0).getMessage().getFormattedMessage(),
        events.get(0).getMessage().getFormattedMessage().contains("busy"));
  }

  private static Exception wrappedTooManyRequests() {
    return new IllegalStateException(
        "Error occurred during join query run",
        new RuntimeException(
            "Error occurred while creating PIT.",
            new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS)));
  }

  @Test
  public void asyncIllegalStateWithoutOpenSearchErrorIsStillMemoryCircuitBreak() throws Exception {
    BytesRestResponse response =
        executeAsyncFailingWith(new IllegalStateException("Memory circuit is broken"));

    assertEquals(RestStatus.INSUFFICIENT_STORAGE, response.status());
    assertEquals("Memory circuit is broken.", response.content().utf8ToString());
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void asyncIOExceptionWithoutOpenSearchErrorIsStillInternalServerError() throws Exception {
    BytesRestResponse response = executeAsyncFailingWith(new IOException("connection reset"));

    assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());
    assertEquals("connection reset", response.content().utf8ToString());
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void asyncUnknownFailureWrappingOpenSearchErrorKeepsItsStatus() throws Exception {
    BytesRestResponse response =
        executeAsyncFailingWith(
            new UnsupportedOperationException(
                new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE)));

    assertEquals(RestStatus.SERVICE_UNAVAILABLE, response.status());
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void asyncUnknownFailureWithoutOpenSearchErrorIsStillInternalServerError()
      throws Exception {
    BytesRestResponse response = executeAsyncFailingWith(new RuntimeException("boom"));

    assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());
    assertEquals("boom", response.content().utf8ToString());
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  private BytesRestResponse executeAsyncFailingWith(Exception failure) throws Exception {
    Metrics.getInstance().clear();
    for (MetricName name :
        new MetricName[] {MetricName.FAILED_REQ_COUNT_CUS, MetricName.FAILED_REQ_COUNT_SYS}) {
      Metrics.getInstance().registerMetric(new NumericMetric<>(name.getName(), new BasicCounter()));
    }
    ThreadPool threadPool = mock(ThreadPool.class);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArgument(0)).run();
              return null;
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), anyString());
    when(client.threadPool()).thenReturn(threadPool);
    doThrow(failure).when(executor).execute(client, params, action, channel);

    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName(TRANSPORT_WORKER_THREAD_NAME_PREFIX);
    try {
      execute();
    } finally {
      Thread.currentThread().setName(originalName);
    }

    ArgumentCaptor<BytesRestResponse> captor = ArgumentCaptor.forClass(BytesRestResponse.class);
    verify(channel).sendResponse(captor.capture());
    return captor.getValue();
  }

  private static long failedRequestCount(MetricName name) {
    return (Long) Metrics.getInstance().getNumericalMetric(name).getValue();
  }
}
