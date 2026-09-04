/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.function.Consumer;
import org.junit.Test;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.RestChannel;
import org.opensearch.tasks.Task;

public class TransportSqlQueryRequestTest {

  @Test
  public void testCreateTaskReturnsSqlQueryTask() {
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", ch -> {}, null);
    Task task =
        request.createTask(1, "transport", SqlQueryAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
    assertNotNull(task);
    assertEquals(SqlQueryTask.class, task.getClass());
  }

  @Test
  public void testDescriptionIsQueryText() {
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", ch -> {}, null);
    assertEquals("SELECT 1", request.getDescription());
  }

  @Test
  public void testDescriptionTruncatedToMaxLength() {
    String longQuery = "x".repeat(TransportSqlQueryRequest.MAX_DESCRIPTION_LENGTH + 100);
    TransportSqlQueryRequest request = new TransportSqlQueryRequest(longQuery, ch -> {}, null);
    assertEquals(
        TransportSqlQueryRequest.MAX_DESCRIPTION_LENGTH, request.getDescription().length());
  }

  @Test
  public void testNullQueryBecomesEmptyDescription() {
    TransportSqlQueryRequest request = new TransportSqlQueryRequest(null, ch -> {}, null);
    assertEquals("", request.getDescription());
  }

  @Test
  public void testCarriesWorkAndChannel() {
    Consumer<RestChannel> work = ch -> {};
    RestChannel channel = mock(RestChannel.class);
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", work, channel);
    assertSame(work, request.getWork());
    assertSame(channel, request.getChannel());
  }

  @Test
  public void testValidateReturnsNull() {
    TransportSqlQueryRequest request = new TransportSqlQueryRequest("SELECT 1", ch -> {}, null);
    assertNull(request.validate());
  }
}
