/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.common.antlr;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A base class for tests for SQL or PPL parser.
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class SyntaxParserTestBase {
  @Getter
  private final Parser parser;

  /**
   * A helper function that fails a test if the parser rejects a given query.
   * @param query Query to test.
   */
  protected void acceptQuery(String query) {
    assertNotNull(parser.parse(query));
  }

  /**
   * A helper function that fails a test if the parser accepts a given query.
   * @param query Query to test.
   */
  protected void rejectQuery(String query) {
    assertThrows(SyntaxCheckException.class, () -> parser.parse(query));
  }
}
