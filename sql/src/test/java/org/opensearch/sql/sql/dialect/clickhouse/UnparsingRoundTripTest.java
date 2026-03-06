/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for round-trip unparsing with specific representative queries.
 *
 * <p>**Validates: Requirements 15.4**
 *
 * <p>For each query, verifies:
 * <ol>
 *   <li>Parse succeeds</li>
 *   <li>Unparse produces non-empty SQL</li>
 *   <li>Re-parse of unparsed SQL succeeds</li>
 *   <li>Double round-trip produces identical SQL (idempotent)</li>
 * </ol>
 */
class UnparsingRoundTripTest {

  private static final OpenSearchClickHouseSqlDialect DIALECT =
      OpenSearchClickHouseSqlDialect.DEFAULT;

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config().withQuoting(Quoting.BACK_TICK).withCaseSensitive(false);

  @Test
  @DisplayName("Simple SELECT: SELECT a, b FROM tbl")
  void simpleSelect() throws SqlParseException {
    assertRoundTrip("SELECT a, b FROM tbl");
  }

  @Test
  @DisplayName("GROUP BY with aggregate: SELECT a, COUNT(*) FROM tbl GROUP BY a")
  void groupByWithAggregate() throws SqlParseException {
    assertRoundTrip("SELECT a, COUNT(*) FROM tbl GROUP BY a");
  }

  @Test
  @DisplayName("WHERE with comparison: SELECT a FROM tbl WHERE a > 10 AND b = 'hello'")
  void whereWithComparison() throws SqlParseException {
    assertRoundTrip("SELECT a FROM tbl WHERE a > 10 AND b = 'hello'");
  }

  @Test
  @DisplayName("ORDER BY with LIMIT: SELECT a FROM tbl ORDER BY a DESC LIMIT 100")
  void orderByWithLimit() throws SqlParseException {
    assertRoundTrip("SELECT a FROM tbl ORDER BY a DESC LIMIT 100");
  }

  @Test
  @DisplayName("CASE WHEN: SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END FROM tbl")
  void caseWhen() throws SqlParseException {
    assertRoundTrip("SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END FROM tbl");
  }

  @Test
  @DisplayName("JOIN: SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id")
  void join() throws SqlParseException {
    assertRoundTrip("SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id");
  }

  @Test
  @DisplayName("DISTINCT with aggregate: SELECT DISTINCT a, SUM(b) FROM tbl GROUP BY a")
  void distinctWithAggregate() throws SqlParseException {
    assertRoundTrip("SELECT DISTINCT a, SUM(b) FROM tbl GROUP BY a");
  }

  @Test
  @DisplayName("Subquery: SELECT a FROM tbl WHERE a > (SELECT MIN(a) FROM tbl)")
  void subquery() throws SqlParseException {
    assertRoundTrip("SELECT a FROM tbl WHERE a > (SELECT MIN(a) FROM tbl)");
  }

  // -------------------------------------------------------------------------
  // Helper
  // -------------------------------------------------------------------------

  /**
   * Asserts that a query round-trips correctly through parse → unparse → re-parse,
   * and that a double round-trip produces identical SQL (idempotent).
   */
  private void assertRoundTrip(String query) throws SqlParseException {
    // 1. Parse succeeds
    SqlNode originalNode = parseSql(query);
    assertNotNull(originalNode, "Parse should succeed for: " + query);

    // 2. Unparse produces non-empty SQL
    String unparsed = originalNode.toSqlString(DIALECT).getSql();
    assertNotNull(unparsed, "Unparsed SQL should not be null for: " + query);
    assertFalse(unparsed.isBlank(), "Unparsed SQL should not be blank for: " + query);

    // 3. Re-parse of unparsed SQL succeeds
    SqlNode reparsedNode =
        assertDoesNotThrow(
            () -> parseSql(unparsed),
            "Re-parse should succeed. Original: '" + query + "', Unparsed: '" + unparsed + "'");
    assertNotNull(reparsedNode, "Re-parsed node should not be null for: " + unparsed);

    // 4. Double round-trip produces identical SQL (idempotent)
    String secondUnparsed = reparsedNode.toSqlString(DIALECT).getSql();
    assertEquals(
        unparsed,
        secondUnparsed,
        "Double round-trip should produce identical SQL. "
            + "Original: '"
            + query
            + "', First unparse: '"
            + unparsed
            + "', Second unparse: '"
            + secondUnparsed
            + "'");
  }

  private static SqlNode parseSql(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
    return parser.parseQuery();
  }
}
