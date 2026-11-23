/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.cursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.legacy.cursor.DefaultCursor;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.legacy.util.LogCapture;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.Client;

public class CursorCloseExecutorTest {

  private final Client client = mock(Client.class);
  private final RestChannel channel = mock(RestChannel.class);

  @Before
  public void setUp() throws Exception {
    LocalClusterState.state().setPluginSettings(mock(OpenSearchSettings.class));
    Metrics.getInstance().clear();
    for (MetricName name :
        new MetricName[] {MetricName.FAILED_REQ_COUNT_CUS, MetricName.FAILED_REQ_COUNT_SYS}) {
      Metrics.getInstance().registerMetric(new NumericMetric<>(name.getName(), new BasicCounter()));
    }
    when(channel.request()).thenReturn(mock(RestRequest.class));
    when(channel.newErrorBuilder()).thenAnswer(invocation -> JsonXContent.contentBuilder());
  }

  @Test
  public void closeDeniedReturnsForbidden() throws Exception {
    failPitDeletionWith(
        new OpenSearchSecurityException(
            "no permissions for [indices:data/read/point_in_time/delete]", RestStatus.FORBIDDEN));

    BytesRestResponse response = close();

    assertEquals(RestStatus.FORBIDDEN, response.status());
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void closeDeniedIsLoggedAtWarnWithoutStackTrace() throws Exception {
    failPitDeletionWith(
        new OpenSearchSecurityException(
            "no permissions for [indices:data/read/point_in_time/delete]", RestStatus.FORBIDDEN));

    List<LogEvent> events = LogCapture.capture(CursorCloseExecutor.class, this::close);

    assertEquals(events.toString(), 1, events.size());
    assertEquals(Level.WARN, events.get(0).getLevel());
    assertNull(events.get(0).getThrown());
  }

  @Test
  public void closeRejectedReturnsTooManyRequests() throws Exception {
    failPitDeletionWith(new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS));

    BytesRestResponse response = close();

    assertEquals(RestStatus.TOO_MANY_REQUESTS, response.status());
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void closeServerErrorStillReportsNotSucceeded() throws Exception {
    failPitDeletionWith(
        new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE));

    BytesRestResponse response = close();

    assertEquals(RestStatus.OK, response.status());
    assertEquals("{\"succeeded\":false}", response.content().utf8ToString());
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  private void failPitDeletionWith(Exception failure) throws Exception {
    ActionFuture<DeletePitResponse> future = mock(ActionFuture.class);
    when(future.get()).thenThrow(new ExecutionException(failure));
    when(client.execute(any(DeletePitAction.class), any(DeletePitRequest.class)))
        .thenReturn(future);
  }

  private BytesRestResponse close() throws Exception {
    DefaultCursor cursor = new DefaultCursor();
    cursor.setRowsLeft(10);
    cursor.setFetchSize(5);
    cursor.setPitId("pit-id");
    cursor.setIndexPattern("logs");
    cursor.setIndices(new String[] {"logs"});
    cursor.setFieldAliasMap(Collections.emptyMap());
    cursor.setColumns(Collections.emptyList());
    cursor.setSortFields(new Object[0]);
    cursor.setSearchSourceBuilder(new SearchSourceBuilder());

    new CursorCloseExecutor(cursor.generateCursorId())
        .execute(client, Collections.emptyMap(), channel);

    ArgumentCaptor<BytesRestResponse> captor = ArgumentCaptor.forClass(BytesRestResponse.class);
    verify(channel).sendResponse(captor.capture());
    return captor.getValue();
  }

  private static long failedRequestCount(MetricName name) {
    return (Long) Metrics.getInstance().getNumericalMetric(name).getValue();
  }
}
