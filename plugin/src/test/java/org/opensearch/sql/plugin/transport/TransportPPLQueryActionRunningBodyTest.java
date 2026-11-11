/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.QueryProgress;
import org.opensearch.sql.job.QueryResult;

public class TransportPPLQueryActionRunningBodyTest {

  @Test
  public void runningBodyKeepsTheIdWhenRowsCannotBeFormatted() throws Exception {
    ExprValue broken = mock(ExprValue.class);
    when(broken.tupleValue()).thenThrow(new IllegalStateException("unformattable"));
    QueryResult.Rows rows =
        new QueryResult.Rows(new Schema(List.of()), List.of(broken), Cursor.None, List.of(), 0);
    QueryJobId id = new QueryJobId("node", "ctx");
    QueryResult.Running running =
        new QueryResult.Running(id, new QueryProgress(0.5D), Optional.of(rows));

    AtomicReference<String> body = new AtomicReference<>();
    ActionListener<TransportPPLQueryResponse> listener =
        ActionListener.wrap(
            response -> body.set(response.getResult()),
            e -> {
              throw new AssertionError(e);
            });
    Method respondRunning =
        TransportPPLQueryAction.class.getDeclaredMethod(
            "respondRunning", QueryResult.Running.class, ActionListener.class);
    respondRunning.setAccessible(true);
    respondRunning.invoke(null, running, listener);

    JSONObject json = new JSONObject(body.get());
    assertEquals(id.encode(), json.getString("id"));
    assertEquals("RUNNING", json.getString("status"));
    assertEquals(0, json.getJSONArray("datarows").length());
    assertFalse(json.has("warnings"));
  }
}
