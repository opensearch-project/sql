/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.search;

import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;
import org.opensearch.sql.api.spec.UnifiedSqlSpec;

/** Unit tests for {@link SelectItemAliasRewriter}. */
public class SelectItemAliasRewriterTest {

  /** Production parser config from {@link UnifiedSqlSpec}. */
  private static final SqlParser.Config PARSER_CONFIG = UnifiedSqlSpec.extended().parserConfig();

  @Test
  public void testAliasesUnnamedExpression() throws Exception {
    SqlNode result = rewrite("SELECT COUNT(*) FROM t");
    assertContains(result, "AS `COUNT(*)`");
  }

  @Test
  public void testPreservesExplicitAlias() throws Exception {
    SqlNode result = rewrite("SELECT COUNT(*) AS cnt FROM t");
    assertContains(result, "SELECT COUNT(*) AS `cnt`");
  }

  @Test
  public void testSkipsSimpleIdentifier() throws Exception {
    SqlNode result = rewrite("SELECT name FROM t");
    assertContains(result, "SELECT `name`");
  }

  @Test
  public void testSkipsStar() throws Exception {
    SqlNode result = rewrite("SELECT * FROM t");
    assertContains(result, "SELECT *");
  }

  @Test
  public void testSkipsQualifiedStar() throws Exception {
    SqlNode result = rewrite("SELECT t.* FROM t");
    assertContains(result, "SELECT `t`.*");
  }

  @Test
  public void testSkipsScalarSubquery() throws Exception {
    // Avoids producing a column name like "SELECT MAX(x) AS MAX(x) FROM u" from our own
    // recursion. Calcite will fall back to EXPR$N, matching Flink/Druid/Drill default.
    SqlNode result = rewrite("SELECT (SELECT MAX(x) FROM u) FROM t");
    assertContains(result, "SELECT (SELECT MAX(`x`) AS `MAX(x)` FROM `u`) FROM `t`");
  }

  @Test
  public void testRecursesIntoSubquery() throws Exception {
    SqlNode result = rewrite("SELECT x FROM (SELECT COUNT(*) FROM t) s");
    assertContains(result, "AS `COUNT(*)`");
  }

  @Test
  public void testHandlesMixedItemsCorrectly() throws Exception {
    SqlNode result = rewrite("SELECT id, COUNT(*), name AS n FROM t");
    assertContains(result, "SELECT `id`, COUNT(*) AS `COUNT(*)`, `name` AS `n`");
  }

  @Test
  public void testPreservesIdentifierInAggregate() throws Exception {
    SqlNode result = rewrite("SELECT SUM(MyCol), SUM(x + 1) FROM t");
    assertContains(result, "AS `SUM(MyCol)`");
    assertContains(result, "AS `SUM(x + 1)`");
  }

  private static SqlNode rewrite(String sql) throws Exception {
    return parse(sql).accept(SelectItemAliasRewriter.INSTANCE);
  }

  private static SqlNode parse(String sql) throws Exception {
    return SqlParser.create(sql, PARSER_CONFIG).parseStmt();
  }

  private static void assertContains(SqlNode node, String expected) {
    String actual = node.toString().replaceAll("\\r\\n", "\n").replaceAll("\\n", " ");
    assertTrue(
        "Expected to contain: " + expected + "\nActual: " + actual, actual.contains(expected));
  }
}
