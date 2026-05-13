/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/**
 * Tests for V2 extended SQL grammar features: JOIN, UNION, and MINUS (EXCEPT). These features are
 * parsed by the V2 ANTLR grammar but throw SyntaxCheckException in the base AstBuilder to trigger
 * legacy engine fallback. The extended AstBuilder in the unified query path handles them.
 */
class SQLSyntaxParserV2ExtensionTest {

  private final SQLSyntaxParser parser = new SQLSyntaxParser();

  @Test
  void canParseInnerJoin() {
    assertNotNull(parser.parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"));
  }

  @Test
  void canParseLeftJoin() {
    assertNotNull(parser.parse("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id"));
  }

  @Test
  void canParseRightJoin() {
    assertNotNull(parser.parse("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id"));
  }

  @Test
  void canParseCrossJoin() {
    assertNotNull(parser.parse("SELECT * FROM t1 CROSS JOIN t2"));
  }

  @Test
  void canParseUnionAll() {
    assertNotNull(parser.parse("SELECT a FROM t1 UNION ALL SELECT b FROM t2"));
  }

  @Test
  void canParseUnion() {
    assertNotNull(parser.parse("SELECT a FROM t1 UNION SELECT b FROM t2"));
  }

  @Test
  void canParseMinus() {
    assertNotNull(parser.parse("SELECT a FROM t1 MINUS SELECT b FROM t2"));
  }

  @Test
  void canParseMultiWayUnion() {
    assertNotNull(
        parser.parse("SELECT a FROM t1 UNION ALL SELECT b FROM t2 UNION ALL SELECT c FROM t3"));
  }
}
