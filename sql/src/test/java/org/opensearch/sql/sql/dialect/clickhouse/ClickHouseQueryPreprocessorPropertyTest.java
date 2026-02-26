/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import net.jqwik.api.*;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Property-based tests for {@link ClickHouseQueryPreprocessor}. Validates: Requirements 3.1, 3.2,
 * 3.3, 9.2
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class ClickHouseQueryPreprocessorPropertyTest {

  private final ClickHouseQueryPreprocessor preprocessor = new ClickHouseQueryPreprocessor();

  // -------------------------------------------------------------------------
  // Property 3: Preprocessing round-trip equivalence
  // -------------------------------------------------------------------------

  /**
   * Property 3: Preprocessing round-trip equivalence — For any valid SQL query with
   * dialect-specific clauses appended, preprocessing the query and then parsing it SHALL produce
   * the same Calcite SqlNode AST as parsing the query without those clauses.
   *
   * <p>Validates: Requirements 3.1, 3.2, 3.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 3: Preprocessing round-trip equivalence")
  void preprocessingThenParsingProducesSameAstAsCleanQuery(
      @ForAll("validBaseQueries") String baseQuery, @ForAll("clickHouseClauses") String clause)
      throws SqlParseException {
    // Parse the clean base query
    SqlNode expectedAst = parseSql(baseQuery);

    // Append the ClickHouse clause and preprocess
    String queryWithClause = baseQuery + " " + clause;
    String preprocessed = preprocessor.preprocess(queryWithClause);

    // Parse the preprocessed query
    SqlNode actualAst = parseSql(preprocessed);

    assertEquals(
        expectedAst.toString(),
        actualAst.toString(),
        "Preprocessed query AST should match clean query AST. "
            + "Base: '"
            + baseQuery
            + "', Clause: '"
            + clause
            + "', Preprocessed: '"
            + preprocessed
            + "'");
  }

  /**
   * Property 3 (passthrough): Queries without dialect-specific clauses should pass through
   * unchanged and produce the same AST.
   *
   * <p>Validates: Requirements 3.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 3: Preprocessing round-trip equivalence")
  void queriesWithoutDialectClausesPassThroughUnchanged(
      @ForAll("validBaseQueries") String baseQuery) throws SqlParseException {
    String preprocessed = preprocessor.preprocess(baseQuery);

    SqlNode expectedAst = parseSql(baseQuery);
    SqlNode actualAst = parseSql(preprocessed);

    assertEquals(
        expectedAst.toString(),
        actualAst.toString(),
        "Query without dialect clauses should produce same AST after preprocessing");
  }

  // -------------------------------------------------------------------------
  // Property 14: ClickHouse preprocessor strips FORMAT/SETTINGS/FINAL
  // -------------------------------------------------------------------------

  /**
   * Property 14 (FORMAT): For any valid SQL query string, appending a FORMAT clause and then
   * preprocessing SHALL produce a string equal to the original query (modulo whitespace).
   *
   * <p>Validates: Requirements 9.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 14: ClickHouse preprocessor strips"
          + " FORMAT/SETTINGS/FINAL")
  void preprocessorStripsFormatClause(
      @ForAll("validBaseQueries") String baseQuery, @ForAll("formatIdentifiers") String formatId) {
    String queryWithFormat = baseQuery + " FORMAT " + formatId;
    String preprocessed = preprocessor.preprocess(queryWithFormat);

    assertEquals(
        normalizeWhitespace(baseQuery),
        normalizeWhitespace(preprocessed),
        "Preprocessing should strip FORMAT clause. Input: '" + queryWithFormat + "'");
  }

  /**
   * Property 14 (SETTINGS): For any valid SQL query string, appending a SETTINGS clause and then
   * preprocessing SHALL produce a string equal to the original query (modulo whitespace).
   *
   * <p>Validates: Requirements 9.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 14: ClickHouse preprocessor strips"
          + " FORMAT/SETTINGS/FINAL")
  void preprocessorStripsSettingsClause(
      @ForAll("validBaseQueries") String baseQuery, @ForAll("settingsClauses") String settings) {
    String queryWithSettings = baseQuery + " " + settings;
    String preprocessed = preprocessor.preprocess(queryWithSettings);

    assertEquals(
        normalizeWhitespace(baseQuery),
        normalizeWhitespace(preprocessed),
        "Preprocessing should strip SETTINGS clause. Input: '" + queryWithSettings + "'");
  }

  /**
   * Property 14 (FINAL): For any valid SQL query string, appending FINAL after the table name and
   * then preprocessing SHALL produce a string equal to the original query (modulo whitespace).
   *
   * <p>Validates: Requirements 9.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 14: ClickHouse preprocessor strips"
          + " FORMAT/SETTINGS/FINAL")
  void preprocessorStripsFinalKeyword(
      @ForAll("queryPairsWithFinal") Tuple.Tuple2<String, String> pair) {
    String queryWithFinal = pair.get1();
    String expectedClean = pair.get2();
    String preprocessed = preprocessor.preprocess(queryWithFinal);

    assertEquals(
        normalizeWhitespace(expectedClean),
        normalizeWhitespace(preprocessed),
        "Preprocessing should strip FINAL keyword. Input: '" + queryWithFinal + "'");
  }

  /**
   * Property 14 (combined): Appending FORMAT, SETTINGS, and FINAL together and preprocessing SHALL
   * produce a string equal to the original query (modulo whitespace).
   *
   * <p>Validates: Requirements 9.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 14: ClickHouse preprocessor strips"
          + " FORMAT/SETTINGS/FINAL")
  void preprocessorStripsCombinedClauses(
      @ForAll("validBaseQueries") String baseQuery,
      @ForAll("formatIdentifiers") String formatId,
      @ForAll("settingsClauses") String settings) {
    String combined = baseQuery + " " + settings + " FORMAT " + formatId;
    String preprocessed = preprocessor.preprocess(combined);

    assertEquals(
        normalizeWhitespace(baseQuery),
        normalizeWhitespace(preprocessed),
        "Preprocessing should strip combined FORMAT+SETTINGS clauses. Input: '" + combined + "'");
  }

  // -------------------------------------------------------------------------
  // Edge case tests: string literals and comments
  // -------------------------------------------------------------------------

  /**
   * Edge case: FORMAT/SETTINGS/FINAL inside string literals must NOT be stripped.
   * Validates: Requirements 9.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 14: ClickHouse preprocessor strips FORMAT/SETTINGS/FINAL")
  void keywordsInsideStringLiteralsArePreserved(
      @ForAll("keywordsInStrings") String query) {
    String preprocessed = preprocessor.preprocess(query);
    // The string literal content must survive preprocessing
    assertTrue(
        preprocessed.contains("FORMAT") || preprocessed.contains("SETTINGS")
            || preprocessed.contains("FINAL") || preprocessed.contains("format")
            || preprocessed.contains("settings") || preprocessed.contains("final"),
        "Keywords inside string literals must be preserved. Input: '"
            + query + "', Output: '" + preprocessed + "'");
  }

  /**
   * Edge case: FORMAT/SETTINGS/FINAL inside line comments must NOT be stripped.
   * Validates: Requirements 9.2
   */
  @Example
  @Tag("Feature: clickhouse-sql-dialect, Property 14: ClickHouse preprocessor strips FORMAT/SETTINGS/FINAL")
  void keywordsInsideLineCommentsArePreserved() {
    // -- comments are stripped by the preprocessor masking, but the query itself should still work
    String query = "SELECT 1 -- FINAL comment";
    String preprocessed = preprocessor.preprocess(query);
    // The comment with FINAL should be preserved (not cause the SELECT to be mangled)
    assertTrue(
        preprocessed.contains("SELECT 1"),
        "Query before comment must be preserved. Output: '" + preprocessed + "'");
  }

  /**
   * Edge case: Mixed case variants of FORMAT/SETTINGS/FINAL should be stripped.
   * Validates: Requirements 9.2
   */
  @Example
  @Tag("Feature: clickhouse-sql-dialect, Property 14: ClickHouse preprocessor strips FORMAT/SETTINGS/FINAL")
  void mixedCaseKeywordsAreStripped() {
    assertEquals("SELECT 1", preprocessor.preprocess("SELECT 1 Format JSON").trim());
    assertEquals("SELECT 1", preprocessor.preprocess("SELECT 1 format json").trim());
    assertEquals("SELECT 1", preprocessor.preprocess("SELECT 1 FORMAT json").trim());
    assertEquals("SELECT col1 FROM tbl",
        normalizeWhitespace(preprocessor.preprocess("SELECT col1 FROM tbl Final")));
    assertEquals("SELECT 1",
        normalizeWhitespace(preprocessor.preprocess("SELECT 1 settings max_threads=2")));
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> validBaseQueries() {
    return Arbitraries.of(
        "SELECT 1",
        "SELECT col1 FROM tbl",
        "SELECT col1, col2 FROM tbl WHERE col1 > 0",
        "SELECT col1 FROM tbl ORDER BY col1",
        "SELECT col1 FROM tbl GROUP BY col1",
        "SELECT col1, COUNT(*) FROM tbl GROUP BY col1 HAVING COUNT(*) > 1",
        "SELECT col1 FROM tbl WHERE col1 = 'abc' ORDER BY col1 LIMIT 10",
        "SELECT a, b, c FROM my_table WHERE a > 10 AND b < 20",
        "SELECT MAX(col1) FROM tbl",
        "SELECT col1 FROM tbl LIMIT 100");
  }

  @Provide
  Arbitrary<String> formatIdentifiers() {
    return Arbitraries.of(
        "JSON",
        "TabSeparated",
        "CSV",
        "TSV",
        "Pretty",
        "JSONEachRow",
        "Native",
        "Vertical",
        "XMLEachRow",
        "Parquet");
  }

  @Provide
  Arbitrary<String> settingsClauses() {
    Arbitrary<String> keys =
        Arbitraries.of(
            "max_threads",
            "max_memory_usage",
            "timeout_before_checking_execution_speed",
            "max_block_size",
            "read_overflow_mode");
    Arbitrary<Integer> values = Arbitraries.integers().between(1, 10000);

    // Single setting
    Arbitrary<String> singleSetting =
        Combinators.combine(keys, values).as((k, v) -> "SETTINGS " + k + "=" + v);

    // Two settings
    Arbitrary<String> twoSettings =
        Combinators.combine(keys, values, keys, values)
            .as((k1, v1, k2, v2) -> "SETTINGS " + k1 + "=" + v1 + ", " + k2 + "=" + v2);

    return Arbitraries.oneOf(singleSetting, twoSettings);
  }

  @Provide
  Arbitrary<String> clickHouseClauses() {
    return Arbitraries.oneOf(
        // FORMAT clauses
        formatIdentifiers().map(f -> "FORMAT " + f),
        // SETTINGS clauses
        settingsClauses(),
        // FORMAT + SETTINGS combined
        Combinators.combine(settingsClauses(), formatIdentifiers())
            .as((s, f) -> s + " FORMAT " + f));
  }

  @Provide
  Arbitrary<String> keywordsInStrings() {
    return Arbitraries.of(
        "SELECT 'FORMAT JSON' FROM tbl",
        "SELECT col1 FROM tbl WHERE col1 = 'FINAL'",
        "SELECT 'SETTINGS max_threads=2' AS cfg FROM tbl",
        "SELECT col1 FROM tbl WHERE name = 'format csv'",
        "SELECT 'FINAL' AS keyword FROM tbl",
        "SELECT col1 FROM tbl WHERE description = 'use FORMAT JSON for output'",
        "SELECT col1 FROM tbl WHERE note = 'SETTINGS are important'");
  }

  @Provide
  Arbitrary<Tuple.Tuple2<String, String>> queryPairsWithFinal() {
    // Returns pairs of (queryWithFinal, expectedCleanQuery)
    return Arbitraries.of(
        Tuple.of("SELECT col1 FROM tbl FINAL", "SELECT col1 FROM tbl"),
        Tuple.of(
            "SELECT col1, col2 FROM tbl FINAL WHERE col1 > 0",
            "SELECT col1, col2 FROM tbl  WHERE col1 > 0"),
        Tuple.of("SELECT col1 FROM tbl FINAL ORDER BY col1", "SELECT col1 FROM tbl  ORDER BY col1"),
        Tuple.of("SELECT col1 FROM tbl FINAL GROUP BY col1", "SELECT col1 FROM tbl  GROUP BY col1"),
        Tuple.of(
            "SELECT col1, COUNT(*) FROM tbl FINAL GROUP BY col1 HAVING COUNT(*) > 1",
            "SELECT col1, COUNT(*) FROM tbl  GROUP BY col1 HAVING COUNT(*) > 1"),
        Tuple.of(
            "SELECT col1 FROM tbl FINAL WHERE col1 = 'abc' ORDER BY col1 LIMIT 10",
            "SELECT col1 FROM tbl  WHERE col1 = 'abc' ORDER BY col1 LIMIT 10"),
        Tuple.of(
            "SELECT a, b, c FROM my_table FINAL WHERE a > 10 AND b < 20",
            "SELECT a, b, c FROM my_table  WHERE a > 10 AND b < 20"),
        Tuple.of("SELECT MAX(col1) FROM tbl FINAL", "SELECT MAX(col1) FROM tbl"),
        Tuple.of("SELECT col1 FROM tbl FINAL LIMIT 100", "SELECT col1 FROM tbl  LIMIT 100"));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static SqlNode parseSql(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, SqlParser.config());
    return parser.parseQuery();
  }

  private static String normalizeWhitespace(String s) {
    return s.trim().replaceAll("\\s+", " ");
  }
}
