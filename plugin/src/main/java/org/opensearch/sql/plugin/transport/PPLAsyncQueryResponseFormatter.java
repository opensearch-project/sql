/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;

import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.UpdateMode;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;

/** Builds the public JSON contract for PPL progressive query execution. */
final class PPLAsyncQueryResponseFormatter {
  private PPLAsyncQueryResponseFormatter() {}

  static TransportPPLQueryResponse fastPath(PPLAsyncQueryJobService.Snapshot snapshot) {
    JSONObject json = rows(snapshot.response(), 0, Integer.MAX_VALUE, false);
    json.put("status", snapshot.status().name());
    json.put("took", snapshot.tookMillis());
    json.put("start_time_in_millis", snapshot.startTimeMillis());
    json.put("progress", new JSONObject().put("fraction_done", snapshot.progress().fractionDone()));
    return new TransportPPLQueryResponse(json.toString(2));
  }

  static TransportPPLQueryResponse job(
      PPLAsyncQueryJobService.Snapshot snapshot, int offset, int count) {
    return job(snapshot, offset, count, true);
  }

  static TransportPPLQueryResponse submission(PPLAsyncQueryJobService.Snapshot snapshot) {
    return job(snapshot, 0, PPLAsyncQueryJobService.DEFAULT_PAGE_SIZE, false);
  }

  private static TransportPPLQueryResponse job(
      PPLAsyncQueryJobService.Snapshot snapshot, int offset, int count, boolean pollResponse) {
    boolean runningReplace =
        snapshot.status() == PPLAsyncQueryJobService.Status.RUNNING
            && snapshot.classified()
            && snapshot.updateMode() == UpdateMode.REPLACE;
    JSONObject json =
        rows(
            snapshot.response(),
            runningReplace ? 0 : offset,
            runningReplace ? Integer.MAX_VALUE : count,
            pollResponse && !runningReplace);
    json.put("id", snapshot.id());
    json.put("status", snapshot.status().name());
    json.put("start_time_in_millis", snapshot.startTimeMillis());
    json.put("expiration_time_in_millis", snapshot.expirationTimeMillis());
    if (snapshot.classified()) {
      json.put("update_mode", snapshot.updateMode().name());
    }
    json.put("progress", new JSONObject().put("fraction_done", snapshot.progress().fractionDone()));
    if (snapshot.status() != PPLAsyncQueryJobService.Status.RUNNING) {
      json.put("took", snapshot.tookMillis());
    }
    if (snapshot.failure() != null) {
      json.put(
          "error",
          new JSONObject()
              .put("type", snapshot.failure().getClass().getSimpleName())
              .put("reason", snapshot.failure().getMessage()));
    }
    return new TransportPPLQueryResponse(json.toString(2));
  }

  static TransportPPLQueryResponse deleted(PPLAsyncQueryJobService.Snapshot snapshot) {
    return new TransportPPLQueryResponse(
        new JSONObject()
            .put("id", snapshot.id())
            .put("status", snapshot.status().name())
            .toString(2));
  }

  private static JSONObject rows(
      ExecutionEngine.QueryResponse response, int offset, int count, boolean includeWindow) {
    if (response == null) {
      JSONObject empty =
          new JSONObject()
              .put("schema", new JSONArray())
              .put("datarows", new JSONArray())
              .put("total", 0)
              .put("size", 0);
      if (includeWindow) {
        empty.put("window", new JSONObject().put("offset", offset).put("count", count));
      }
      return empty;
    }

    List<org.opensearch.sql.data.model.ExprValue> allRows = response.getResults();
    int from = Math.min(offset, allRows.size());
    int to = (int) Math.min(allRows.size(), (long) from + count);
    ExecutionEngine.QueryResponse windowed =
        new ExecutionEngine.QueryResponse(
            response.getSchema(), List.copyOf(allRows.subList(from, to)), null);
    windowed.setWarnings(List.copyOf(response.getWarnings()));
    SimpleJsonResponseFormatter formatter =
        new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
    JSONObject json =
        new JSONObject(
            formatter.format(
                new QueryResult(
                    windowed.getSchema(),
                    windowed.getResults(),
                    null,
                    PPL_SPEC,
                    windowed.getWarnings())));
    json.put("total", allRows.size());
    json.put("size", to - from);
    if (includeWindow) {
      json.put("window", new JSONObject().put("offset", offset).put("count", count));
    }
    return json;
  }
}
