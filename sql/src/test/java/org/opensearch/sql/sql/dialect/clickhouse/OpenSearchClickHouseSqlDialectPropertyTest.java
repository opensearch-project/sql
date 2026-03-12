/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import net.jqwik.api.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;

/**
 * Property-based tests for {@link OpenSearchClickHouseSqlDialect}. Validates: Requirements 6.1,
 * 6.2, 6.3
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class OpenSearchClickHouseSqlDialectPropertyTest {

  private static final OpenSearchClickHouseSqlDialect DIALECT =
      OpenSearchClickHouseSqlDialect.DEFAULT;

  // -------------------------------------------------------------------------
  // Property 8: Transpiler produces valid dialect SQL
  // -------------------------------------------------------------------------

  /**
   * Property 8: Transpiler produces valid dialect SQL — For any Calcite RelNode plan, unparsing
   * with a dialect's SqlDialect subclass via UnifiedQueryTranspiler SHALL produce a non-empty SQL
   * string.
   *
   * <p>We parse simple SQL queries into SqlNode trees and unparse them using the ClickHouse
   * dialect. The result must always be a non-empty string.
   *
   * <p>Validates: Requirements 6.1, 6.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 8: Transpiler produces valid dialect SQL")
  void unparsingParsedQueryProducesNonEmptyString(
      @ForAll("validSqlQueries") String query) throws SqlParseException {
    SqlNode node = parseSql(query);
    String result = node.toSqlString(DIALECT).getSql();

    assertNotNull(result, "Unparsed SQL should not be null for query: " + query);
    assertFalse(result.isBlank(), "Unparsed SQL should not be blank for query: " + query);
  }

  /**
   * Property 8 (structure preservation): Unparsing a parsed query should produce SQL that contains
   * the SELECT keyword, confirming structural validity.
   *
   * <p>Validates: Requirements 6.1, 6.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 8: Transpiler produces valid dialect SQL")
  void unparsedSqlContainsSelectKeyword(
      @ForAll("validSqlQueries") String query) throws SqlParseException {
    SqlNode node = parseSql(query);
    String result = node.toSqlString(DIALECT).getSql();

    assertTrue(
        result.toUpperCase().contains("SELECT"),
        "Unparsed SQL should contain SELECT keyword, got: " + result);
  }

  /**
   * Property 8 (round-trip): Parsing and unparsing should produce SQL that can be parsed again
   * without errors. Uses backtick quoting config since the ClickHouse dialect unparses with
   * backtick-quoted identifiers.
   *
   * <p>Validates: Requirements 6.1, 6.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 8: Transpiler produces valid dialect SQL")
  void unparsedSqlCanBeReparsed(
      @ForAll("validSqlQueries") String query) throws SqlParseException {
    SqlNode node = parseSql(query);
    String unparsed = node.toSqlString(DIALECT).getSql();

    // The unparsed SQL should be parseable again using backtick quoting
    // since the ClickHouse dialect unparses identifiers with backticks
    assertDoesNotThrow(
        () -> parseSqlWithBackticks(unparsed),
        "Unparsed SQL should be re-parseable: " + unparsed);
  }

  // -------------------------------------------------------------------------
  // Property 9: Unparse function name mapping
  // -------------------------------------------------------------------------

  /**
   * Property 9: Unparse function name mapping — For any Calcite function call whose operator name
   * is in the dialect's reverse mapping, unparsing SHALL produce SQL containing the dialect-specific
   * function name rather than the Calcite-internal name.
   *
   * <p>Tests COUNT_DISTINCT → uniqExact mapping by creating a SqlCall with the COUNT operator and
   * verifying the unparsed output contains "uniqExact".
   *
   * <p>Validates: Requirements 6.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 9: Unparse function name mapping")
  void countDistinctUnparsesToUniqExact(@ForAll("columnNames") String colName) {
    // Create a COUNT_DISTINCT function call: the operator name must be "COUNT_DISTINCT"
    // to match the CALCITE_TO_CLICKHOUSE_MAPPING key
    SqlFunction countDistinctOp =
        new SqlFunction(
            "COUNT_DISTINCT",
            SqlKind.OTHER_FUNCTION,
            SqlStdOperatorTable.COUNT.getReturnTypeInference(),
            null,
            SqlStdOperatorTable.COUNT.getOperandTypeChecker(),
            SqlFunctionCategory.NUMERIC);

    SqlNode colRef = new SqlIdentifier(colName, SqlParserPos.ZERO);
    SqlCall call = countDistinctOp.createCall(SqlParserPos.ZERO, colRef);

    String result = unparseCall(call);

    assertTrue(
        result.contains("uniqExact"),
        "COUNT_DISTINCT should unparse to uniqExact, got: " + result);
    assertFalse(
        result.contains("COUNT_DISTINCT"),
        "Unparsed SQL should NOT contain COUNT_DISTINCT, got: " + result);
  }

  /**
   * Property 9 (ARRAY_AGG → groupArray): ARRAY_AGG function calls should unparse to groupArray.
   *
   * <p>Validates: Requirements 6.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 9: Unparse function name mapping")
  void arrayAggUnparsesToGroupArray(@ForAll("columnNames") String colName) {
    SqlNode colRef = new SqlIdentifier(colName, SqlParserPos.ZERO);
    SqlCall call = SqlLibraryOperators.ARRAY_AGG.createCall(SqlParserPos.ZERO, colRef);

    String result = unparseCall(call);

    assertTrue(
        result.contains("groupArray"),
        "ARRAY_AGG should unparse to groupArray, got: " + result);
    assertFalse(
        result.contains("ARRAY_AGG"),
        "Unparsed SQL should NOT contain ARRAY_AGG, got: " + result);
  }

  /**
   * Property 9 (DATE_TRUNC → toStartOfInterval): DATE_TRUNC function calls should unparse to
   * toStartOfInterval.
   *
   * <p>Validates: Requirements 6.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 9: Unparse function name mapping")
  void dateTruncUnparsesToToStartOfInterval(@ForAll("columnNames") String colName) {
    // Create a DATE_TRUNC function call
    SqlFunction dateTruncOp =
        new SqlFunction(
            "DATE_TRUNC",
            SqlKind.OTHER_FUNCTION,
            SqlStdOperatorTable.CURRENT_TIMESTAMP.getReturnTypeInference(),
            null,
            null,
            SqlFunctionCategory.TIMEDATE);

    SqlNode unitLiteral = SqlLiteral.createCharString("HOUR", SqlParserPos.ZERO);
    SqlNode colRef = new SqlIdentifier(colName, SqlParserPos.ZERO);
    SqlCall call = dateTruncOp.createCall(SqlParserPos.ZERO, unitLiteral, colRef);

    String result = unparseCall(call);

    assertTrue(
        result.contains("toStartOfInterval"),
        "DATE_TRUNC should unparse to toStartOfInterval, got: " + result);
    assertFalse(
        result.contains("DATE_TRUNC"),
        "Unparsed SQL should NOT contain DATE_TRUNC, got: " + result);
  }

  /**
   * Property 9 (all mapped functions): For any operator name in the CALCITE_TO_CLICKHOUSE_MAPPING,
   * unparsing should produce the ClickHouse-specific name.
   *
   * <p>Validates: Requirements 6.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 9: Unparse function name mapping")
  void allMappedFunctionsUnparseToClickHouseNames(
      @ForAll("mappedFunctionEntries") MappedFunction mapping) {
    SqlFunction op =
        new SqlFunction(
            mapping.calciteName,
            SqlKind.OTHER_FUNCTION,
            SqlStdOperatorTable.COUNT.getReturnTypeInference(),
            null,
            null,
            SqlFunctionCategory.NUMERIC);

    SqlNode colRef = new SqlIdentifier("col", SqlParserPos.ZERO);
    SqlCall call = op.createCall(SqlParserPos.ZERO, colRef);

    String result = unparseCall(call);

    assertTrue(
        result.contains(mapping.clickHouseName),
        mapping.calciteName
            + " should unparse to "
            + mapping.clickHouseName
            + ", got: "
            + result);
  }

  /**
   * Property 9 (non-mapped functions pass through): For function calls whose operator name is NOT
   * in the mapping, unparsing should preserve the original operator name.
   *
   * <p>Validates: Requirements 6.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 9: Unparse function name mapping")
  void nonMappedFunctionsPreserveOriginalName(
      @ForAll("nonMappedFunctionNames") String funcName) {
    SqlFunction op =
        new SqlFunction(
            funcName,
            SqlKind.OTHER_FUNCTION,
            SqlStdOperatorTable.COUNT.getReturnTypeInference(),
            null,
            null,
            SqlFunctionCategory.NUMERIC);

    SqlNode colRef = new SqlIdentifier("col", SqlParserPos.ZERO);
    SqlCall call = op.createCall(SqlParserPos.ZERO, colRef);

    String result = unparseCall(call);

    // The original function name should appear in the output (case may vary)
    assertTrue(
        result.toUpperCase().contains(funcName.toUpperCase()),
        "Non-mapped function " + funcName + " should preserve its name, got: " + result);
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> validSqlQueries() {
    return Arbitraries.of(
        "SELECT 1",
        "SELECT 1 + 2",
        "SELECT col FROM tbl",
        "SELECT a, b FROM tbl WHERE a > 0",
        "SELECT a, COUNT(*) FROM tbl GROUP BY a",
        "SELECT a FROM tbl ORDER BY a",
        "SELECT a FROM tbl LIMIT 10",
        "SELECT a, b, c FROM tbl WHERE a = 1 AND b = 2",
        "SELECT DISTINCT a FROM tbl",
        "SELECT a, SUM(b) FROM tbl GROUP BY a HAVING SUM(b) > 10",
        "SELECT a FROM tbl WHERE a IN (1, 2, 3)",
        "SELECT a FROM tbl WHERE a BETWEEN 1 AND 10",
        "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id",
        "SELECT a FROM tbl WHERE a IS NOT NULL",
        "SELECT CAST(a AS INTEGER) FROM tbl",
        "SELECT a + b AS total FROM tbl");
  }

  @Provide
  Arbitrary<String> columnNames() {
    return Arbitraries.of("col", "value", "ts", "name", "amount", "id", "status", "created_at");
  }

  @Provide
  Arbitrary<MappedFunction> mappedFunctionEntries() {
    return Arbitraries.of(
        new MappedFunction("COUNT_DISTINCT", "uniqExact"),
        new MappedFunction("ARRAY_AGG", "groupArray"),
        new MappedFunction("DATE_TRUNC", "toStartOfInterval"));
  }

  @Provide
  Arbitrary<String> nonMappedFunctionNames() {
    return Arbitraries.of("ABS", "SQRT", "UPPER", "LOWER", "LENGTH", "TRIM", "ROUND", "FLOOR");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static SqlNode parseSql(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, SqlParser.config());
    return parser.parseQuery();
  }

  /**
   * Parse SQL using a config that supports backtick quoting, matching the ClickHouse dialect's
   * unparse output.
   */
  private static SqlNode parseSqlWithBackticks(String sql) throws SqlParseException {
    SqlParser.Config config =
        SqlParser.config()
            .withQuoting(Quoting.BACK_TICK)
            .withUnquotedCasing(Casing.UNCHANGED);
    SqlParser parser = SqlParser.create(sql, config);
    return parser.parseQuery();
  }

  /**
   * Unparse a SqlCall using the ClickHouse dialect and return the resulting SQL string.
   */
  private String unparseCall(SqlCall call) {
    SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
    DIALECT.unparseCall(writer, call, 0, 0);
    return writer.toSqlString().getSql();
  }

  /** Record holding a Calcite function name and its expected ClickHouse equivalent. */
  record MappedFunction(String calciteName, String clickHouseName) {}
}
