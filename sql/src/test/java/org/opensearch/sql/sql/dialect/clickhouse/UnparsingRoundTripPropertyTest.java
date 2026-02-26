/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import net.jqwik.api.*;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Property-based tests for unparsing round-trip (Property 28).
 *
 * <p>**Validates: Requirements 15.4**
 *
 * <p>For all valid ClickHouse queries in the supported subset, parsing to a SqlNode and then
 * unparsing via the ClickHouse SqlDialect SHALL produce SQL that can be re-parsed by Calcite
 * without errors (round-trip unparsing).
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class UnparsingRoundTripPropertyTest {

  private static final OpenSearchClickHouseSqlDialect DIALECT =
      OpenSearchClickHouseSqlDialect.DEFAULT;

  /** ClickHouse dialect parser config: backtick quoting, case insensitive. */
  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config().withQuoting(Quoting.BACK_TICK).withCaseSensitive(false);

  // -------------------------------------------------------------------------
  // Property 28: Unparsing round-trip
  // -------------------------------------------------------------------------

  /**
   * Property 28: For any valid ClickHouse query in the supported subset, parsing and then
   * unparsing via the ClickHouse SqlDialect SHALL produce SQL that can be re-parsed without errors.
   *
   * <p>**Validates: Requirements 15.4**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 28: Unparsing round-trip")
  void parsedQueryUnparsesAndReparsesWithoutErrors(
      @ForAll("clickHouseQueries") String query) throws SqlParseException {
    // Step 1: Parse the original query
    SqlNode originalNode = parseSql(query);
    assertNotNull(originalNode, "Original parse should succeed for: " + query);

    // Step 2: Unparse using the ClickHouse dialect
    String unparsed = originalNode.toSqlString(DIALECT).getSql();
    assertNotNull(unparsed, "Unparsed SQL should not be null for: " + query);
    assertFalse(unparsed.isBlank(), "Unparsed SQL should not be blank for: " + query);

    // Step 3: Re-parse the unparsed SQL
    SqlNode reparsedNode =
        assertDoesNotThrow(
            () -> parseSql(unparsed),
            "Re-parsing unparsed SQL should not throw. Original: '"
                + query
                + "', Unparsed: '"
                + unparsed
                + "'");
    assertNotNull(reparsedNode, "Re-parsed node should not be null for unparsed: " + unparsed);
  }

  /**
   * Property 28 (structural preservation): The re-parsed AST should produce the same unparsed SQL
   * as the original AST (idempotent unparsing after the first round-trip).
   *
   * <p>**Validates: Requirements 15.4**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 28: Unparsing round-trip")
  void doubleRoundTripProducesSameUnparsedSql(
      @ForAll("clickHouseQueries") String query) throws SqlParseException {
    // First round-trip: parse → unparse
    SqlNode firstNode = parseSql(query);
    String firstUnparsed = firstNode.toSqlString(DIALECT).getSql();

    // Second round-trip: re-parse → unparse again
    SqlNode secondNode = parseSql(firstUnparsed);
    String secondUnparsed = secondNode.toSqlString(DIALECT).getSql();

    assertEquals(
        firstUnparsed,
        secondUnparsed,
        "Double round-trip should produce identical unparsed SQL. "
            + "Original: '"
            + query
            + "', First unparse: '"
            + firstUnparsed
            + "', Second unparse: '"
            + secondUnparsed
            + "'");
  }

  /**
   * Property 28 (SELECT keyword preserved): The unparsed SQL should always contain the SELECT
   * keyword, confirming structural validity through the round-trip.
   *
   * <p>**Validates: Requirements 15.4**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 28: Unparsing round-trip")
  void roundTripPreservesSelectKeyword(
      @ForAll("clickHouseQueries") String query) throws SqlParseException {
    SqlNode node = parseSql(query);
    String unparsed = node.toSqlString(DIALECT).getSql();

    assertTrue(
        unparsed.toUpperCase().contains("SELECT"),
        "Unparsed SQL should contain SELECT keyword. Original: '"
            + query
            + "', Unparsed: '"
            + unparsed
            + "'");

    // Re-parse and unparse again — still should contain SELECT
    SqlNode reparsed = parseSql(unparsed);
    String reUnparsed = reparsed.toSqlString(DIALECT).getSql();

    assertTrue(
        reUnparsed.toUpperCase().contains("SELECT"),
        "Re-unparsed SQL should contain SELECT keyword: " + reUnparsed);
  }

  /**
   * Property 28 (generated queries with clauses): For queries generated with various SQL clauses
   * (WHERE, GROUP BY, ORDER BY, LIMIT, HAVING), the round-trip should succeed.
   *
   * <p>**Validates: Requirements 15.4**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 28: Unparsing round-trip")
  void generatedQueriesWithClausesRoundTrip(
      @ForAll("generatedSelectQueries") String query) throws SqlParseException {
    SqlNode originalNode = parseSql(query);
    String unparsed = originalNode.toSqlString(DIALECT).getSql();

    assertDoesNotThrow(
        () -> parseSql(unparsed),
        "Generated query round-trip should succeed. Original: '"
            + query
            + "', Unparsed: '"
            + unparsed
            + "'");
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  /**
   * Provides a set of representative ClickHouse-compatible SQL queries covering various clauses:
   * simple SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, HAVING, JOIN, DISTINCT, BETWEEN, IN,
   * CAST, aliases, and expressions.
   */
  @Provide
  Arbitrary<String> clickHouseQueries() {
    return Arbitraries.of(
        // Simple selects
        "SELECT 1",
        "SELECT 1 + 2",
        "SELECT 1, 2, 3",
        "SELECT col FROM tbl",
        "SELECT a, b, c FROM tbl",
        // WHERE clause
        "SELECT a FROM tbl WHERE a > 0",
        "SELECT a, b FROM tbl WHERE a = 1 AND b = 2",
        "SELECT a FROM tbl WHERE a IS NOT NULL",
        "SELECT a FROM tbl WHERE a IN (1, 2, 3)",
        "SELECT a FROM tbl WHERE a BETWEEN 1 AND 10",
        "SELECT a FROM tbl WHERE a > 0 OR b < 100",
        // GROUP BY
        "SELECT a, COUNT(*) FROM tbl GROUP BY a",
        "SELECT a, SUM(b) FROM tbl GROUP BY a",
        "SELECT a, AVG(b), MIN(c) FROM tbl GROUP BY a",
        // HAVING
        "SELECT a, SUM(b) FROM tbl GROUP BY a HAVING SUM(b) > 10",
        "SELECT a, COUNT(*) AS cnt FROM tbl GROUP BY a HAVING COUNT(*) > 5",
        // ORDER BY
        "SELECT a FROM tbl ORDER BY a",
        "SELECT a, b FROM tbl ORDER BY a ASC, b DESC",
        "SELECT a FROM tbl ORDER BY a ASC",
        // LIMIT
        "SELECT a FROM tbl LIMIT 10",
        "SELECT a FROM tbl ORDER BY a LIMIT 100",
        // DISTINCT
        "SELECT DISTINCT a FROM tbl",
        "SELECT DISTINCT a, b FROM tbl",
        // Aliases
        "SELECT a AS col_a, b AS col_b FROM tbl",
        "SELECT a + b AS total FROM tbl",
        // CAST
        "SELECT CAST(a AS INTEGER) FROM tbl",
        "SELECT CAST(a AS VARCHAR) FROM tbl",
        // JOIN
        "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id",
        "SELECT t1.a, t2.b FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
        // Expressions
        "SELECT a * 2 + 1 FROM tbl",
        "SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END FROM tbl",
        // Subquery in WHERE
        "SELECT a FROM tbl WHERE a > (SELECT MIN(a) FROM tbl)",
        // Multiple aggregates
        "SELECT COUNT(*), SUM(a), AVG(b), MAX(c), MIN(d) FROM tbl");
  }

  /**
   * Generates SELECT queries by combining columns, tables, and optional clauses. This provides
   * more variety than the fixed set above.
   */
  @Provide
  Arbitrary<String> generatedSelectQueries() {
    Arbitrary<String> columns = Arbitraries.of("a", "b", "c", "x", "y", "val", "ts", "id");
    Arbitrary<String> tables = Arbitraries.of("tbl", "t1", "events", "data", "logs");
    Arbitrary<String> optionalWhere =
        Arbitraries.of(
            "",
            " WHERE a > 0",
            " WHERE b = 1",
            " WHERE x IS NOT NULL",
            " WHERE val BETWEEN 1 AND 100",
            " WHERE id IN (1, 2, 3)");
    Arbitrary<String> optionalGroupBy =
        Arbitraries.of("", " GROUP BY a", " GROUP BY b", " GROUP BY a, b");
    Arbitrary<String> optionalOrderBy =
        Arbitraries.of("", " ORDER BY a", " ORDER BY b DESC", " ORDER BY a ASC, b DESC");
    Arbitrary<String> optionalLimit = Arbitraries.of("", " LIMIT 10", " LIMIT 100");

    return Combinators.combine(columns, tables, optionalWhere, optionalGroupBy, optionalOrderBy, optionalLimit)
        .as(
            (col, table, where, groupBy, orderBy, limit) -> {
              StringBuilder sb = new StringBuilder("SELECT ").append(col).append(" FROM ").append(table);
              sb.append(where);
              sb.append(groupBy);
              sb.append(orderBy);
              sb.append(limit);
              return sb.toString();
            });
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static SqlNode parseSql(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
    return parser.parseQuery();
  }
}
