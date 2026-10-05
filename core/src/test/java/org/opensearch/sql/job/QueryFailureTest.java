/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QueryFailureTest {

  @Test
  void of_capturesSimpleTypeAndMessage() {
    QueryFailure failure = QueryFailure.of(new IllegalStateException("boom"));
    assertEquals("IllegalStateException", failure.type());
    assertEquals("boom", failure.reason());
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
