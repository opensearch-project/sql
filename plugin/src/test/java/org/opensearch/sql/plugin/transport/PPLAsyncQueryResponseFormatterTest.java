/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.UpdateMode;

public class PPLAsyncQueryResponseFormatterTest {

  @Test
  public void runningAppendResultIsPageable() {
    JSONObject json =
        json(
            PPLAsyncQueryResponseFormatter.job(
                snapshot(PPLAsyncQueryJobService.Status.RUNNING, UpdateMode.APPEND), 1, 1));

    assertEquals(1, json.getJSONArray("datarows").length());
    assertEquals("row-1", json.getJSONArray("datarows").getJSONArray(0).getString(0));
    assertEquals(2, json.getInt("total"));
    assertEquals(1, json.getJSONObject("window").getInt("offset"));
  }

  @Test
  public void runningReplaceResultIsCompleteAndNotPaged() {
    JSONObject json =
        json(
            PPLAsyncQueryResponseFormatter.job(
                snapshot(PPLAsyncQueryJobService.Status.RUNNING, UpdateMode.REPLACE), 0, 1));

    assertEquals(2, json.getJSONArray("datarows").length());
    assertFalse(json.has("window"));
  }

  @Test
  public void finalReplaceResultIsPageable() {
    JSONObject json =
        json(
            PPLAsyncQueryResponseFormatter.job(
                snapshot(PPLAsyncQueryJobService.Status.SUCCEEDED, UpdateMode.REPLACE), 1, 1));

    assertEquals(1, json.getJSONArray("datarows").length());
    assertTrue(json.has("window"));
    assertEquals(1D, json.getJSONObject("progress").getDouble("fraction_done"), 0D);
  }

  @Test
  public void asyncSubmissionDoesNotExposeAPollWindow() {
    JSONObject json =
        json(
            PPLAsyncQueryResponseFormatter.submission(
                snapshot(PPLAsyncQueryJobService.Status.RUNNING, UpdateMode.APPEND)));

    assertFalse(json.has("window"));
  }

  private static PPLAsyncQueryJobService.Snapshot snapshot(
      PPLAsyncQueryJobService.Status status, UpdateMode updateMode) {
    double progress = status == PPLAsyncQueryJobService.Status.SUCCEEDED ? 1D : 0.5D;
    return new PPLAsyncQueryJobService.Snapshot(
        "id",
        status,
        true,
        updateMode,
        new QueryProgress(progress),
        response(),
        null,
        100L,
        1_000L,
        status == PPLAsyncQueryJobService.Status.RUNNING ? -1L : 900L);
  }

  private static QueryResponse response() {
    return new QueryResponse(
        new Schema(List.of(new Column("value", null, ExprCoreType.STRING))),
        List.of(row("row-0"), row("row-1")),
        null);
  }

  private static ExprValue row(String value) {
    return ExprTupleValue.fromExprValueMap(Map.of("value", ExprValueUtils.stringValue(value)));
  }

  private static JSONObject json(TransportPPLQueryResponse response) {
    return new JSONObject(response.getResult());
  }
}
