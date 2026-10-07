/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.spark.transport.model.AsyncQueryResult;

public class AsyncQueryResultResponseFormatterTest {

  private final ExecutionEngine.Schema schema =
      new ExecutionEngine.Schema(
          ImmutableList.of(
              new ExecutionEngine.Schema.Column("firstname", null, STRING),
              new ExecutionEngine.Schema.Column("age", null, INTEGER)));

  @Test
  void formatSparkQueryResponse() {
    assertSuccessfulResponse("success");
  }

  @Test
  void formatPplQueryResponse() {
    assertSuccessfulResponse("SUCCEEDED");
  }

  private void assertSuccessfulResponse(String status) {
    AsyncQueryResult response =
        new AsyncQueryResult(
            status,
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))),
            null);
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals(
        "{\"status\":\""
            + status
            + "\",\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":"
            + "[[\"John\",20],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  @Test
  void formatAsyncQueryError() {
    AsyncQueryResult response = new AsyncQueryResult("FAILED", null, null, "foo");
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals("{\"status\":\"FAILED\",\"error\":\"foo\"}", formatter.format(response));
  }

  @Test
  void formatAsyncQueryStructuredErrorEmitsObject() {
    // The PPL FAILED path populates errorDetails with the sync-shape error map; the formatter
    // must emit it as a JSON object, not a string.
    java.util.LinkedHashMap<String, Object> details = new java.util.LinkedHashMap<>();
    details.put("type", "IllegalArgumentException");
    details.put("reason", "Field [x] not found.");
    details.put("code", "FIELD_NOT_FOUND");
    AsyncQueryResult response = new AsyncQueryResult("FAILED", null, null, "fallback", details);
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals(
        "{\"status\":\"FAILED\",\"error\":{\"type\":\"IllegalArgumentException\","
            + "\"reason\":\"Field [x] not found.\",\"code\":\"FIELD_NOT_FOUND\"}}",
        formatter.format(response));
  }

  @Test
  void formatAsyncQueryEmptyErrorDetailsFallsBackToErrorString() {
    // Guard the "no renderer / empty map" case: formatter must not emit `"error": {}` and must
    // not swallow the Spark-style string error.
    AsyncQueryResult response =
        new AsyncQueryResult("FAILED", null, null, "fallback", java.util.Map.of());
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals("{\"status\":\"FAILED\",\"error\":\"fallback\"}", formatter.format(response));
  }
}
