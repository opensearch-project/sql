/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QueryFailureTest {

  @Test
  void of_capturesSimpleTypeAndMessage() {
    QueryFailure failure = QueryFailure.of(new IllegalStateException("boom"));
    assertEquals("IllegalStateException", failure.type());
    assertEquals("boom", failure.reason());
    assertEquals(Map.of(), failure.details());
  }

  @Test
  void of_appliesRendererForStructuredDetails() {
    Throwable cause = new IllegalArgumentException("Field [x] not found.");
    QueryFailure failure =
        QueryFailure.of(cause, t -> Map.of("code", "FIELD_NOT_FOUND", "reason", t.getMessage()));
    assertEquals("FIELD_NOT_FOUND", failure.details().get("code"));
    assertEquals("Field [x] not found.", failure.details().get("reason"));
  }

  @Test
  void of_swallowsRendererFailures() {
    QueryFailure failure =
        QueryFailure.of(
            new IllegalStateException("boom"),
            t -> {
              throw new RuntimeException("renderer bug");
            });
    assertEquals(Map.of(), failure.details());
  }

  @Test
  void of_treatsNullRendererAsNoDetails() {
    QueryFailure failure = QueryFailure.of(new RuntimeException("x"), null);
    assertEquals(Map.of(), failure.details());
  }

  @Test
  void of_treatsNullRenderedMapAsNoDetails() {
    QueryFailure failure = QueryFailure.of(new RuntimeException("x"), t -> null);
    assertEquals(Map.of(), failure.details());
  }

  @Test
  void details_isDefensivelyCopiedFromCaller() {
    Map<String, Object> source = new HashMap<>();
    source.put("code", "FIELD_NOT_FOUND");
    QueryFailure failure = new QueryFailure("T", "r", source);
    source.put("code", "mutated-after");
    assertEquals("FIELD_NOT_FOUND", failure.details().get("code"));
    assertThrows(UnsupportedOperationException.class, () -> failure.details().put("x", "y"));
  }

  @Test
  void details_rejectsNull() {
    assertThrows(NullPointerException.class, () -> new QueryFailure("T", "r", null));
  }

  @Test
  void of_fallsBackWhenMessageIsBlank() {
    QueryFailure failure = QueryFailure.of(new RuntimeException(""));
    assertEquals("query execution failed", failure.reason());
  }

  @Test
  void of_fallsBackWhenMessageIsNull() {
    QueryFailure failure = QueryFailure.of(new RuntimeException());
    assertEquals("query execution failed", failure.reason());
  }

  @Test
  void of_usesFullNameForAnonymousException() {
    Throwable anon = new Throwable("anon") {};
    QueryFailure failure = QueryFailure.of(anon);
    assertEquals(anon.getClass().getName(), failure.type());
  }
}
