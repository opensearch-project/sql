/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import net.jqwik.api.*;

/**
 * Property-based tests for token-aware preprocessing in {@link ClickHouseQueryPreprocessor}.
 *
 * <p><b>Property 23: Token-aware preprocessing preserves non-top-level keywords</b>
 *
 * <p>For any valid SQL query where FORMAT, SETTINGS, or FINAL tokens appear inside string literals,
 * block comments, line comments, or within parenthesized expressions (function args, subqueries),
 * preprocessing SHALL preserve those tokens unchanged. Only top-level (depth-0, outside
 * strings/comments) occurrences SHALL be stripped.
 *
 * <p><b>Validates: Requirements 11.1, 11.2, 11.3, 11.4, 11.5</b>
 */
class TokenAwarePreprocessingPropertyTest {

  private final ClickHouseQueryPreprocessor preprocessor = new ClickHouseQueryPreprocessor();

  // -------------------------------------------------------------------------
  // Property 23a: Keywords inside string literals are preserved
  // Validates: Requirement 11.1
  // -------------------------------------------------------------------------

  /**
   * For any dialect keyword appearing inside a single-quoted string literal, preprocessing SHALL
   * preserve the string literal unchanged.
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 23: Token-aware preprocessing preserves"
          + " non-top-level keywords")
  void keywordsInsideStringLiteralsArePreserved(
      @ForAll("baseQueries") String base,
      @ForAll("dialectKeywords") String keyword,
      @ForAll("keywordCaseVariants") String caseVariant) {
    // Build a query with the keyword inside a string literal
    String query = base.replace("FROM tbl", "FROM tbl WHERE col1 = '" + caseVariant + "'");
    String preprocessed = preprocessor.preprocess(query);

    assertTrue(
        preprocessed.contains("'" + caseVariant + "'"),
        "Keyword '"
            + caseVariant
            + "' inside string literal must be preserved. "
            + "Input: '"
            + query
            + "', Output: '"
            + preprocessed
            + "'");
  }

  // -------------------------------------------------------------------------
  // Property 23b: Keywords inside block comments are preserved
  // Validates: Requirement 11.2
  // -------------------------------------------------------------------------

  /**
   * For any dialect keyword appearing inside a block comment, preprocessing SHALL preserve the
   * comment unchanged.
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 23: Token-aware preprocessing preserves"
          + " non-top-level keywords")
  void keywordsInsideBlockCommentsArePreserved(
      @ForAll("baseQueries") String base,
      @ForAll("keywordCaseVariants") String caseVariant) {
    String comment = "/* " + caseVariant + " JSON */";
    String query = base.replace("SELECT", "SELECT " + comment);
    String preprocessed = preprocessor.preprocess(query);

    assertTrue(
        preprocessed.contains(comment),
        "Block comment containing '"
            + caseVariant
            + "' must be preserved. "
            + "Input: '"
            + query
            + "', Output: '"
            + preprocessed
            + "'");
  }

  // -------------------------------------------------------------------------
  // Property 23c: Keywords inside line comments are preserved
  // Validates: Requirement 11.2
  // -------------------------------------------------------------------------

  /**
   * For any dialect keyword appearing inside a line comment, preprocessing SHALL preserve the
   * comment unchanged.
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 23: Token-aware preprocessing preserves"
          + " non-top-level keywords")
  void keywordsInsideLineCommentsArePreserved(
      @ForAll("baseQueries") String base,
      @ForAll("keywordCaseVariants") String caseVariant) {
    String query = base + " -- " + caseVariant + " clause here";
    String preprocessed = preprocessor.preprocess(query);

    assertTrue(
        preprocessed.contains("-- " + caseVariant),
        "Line comment containing '"
            + caseVariant
            + "' must be preserved. "
            + "Input: '"
            + query
            + "', Output: '"
            + preprocessed
            + "'");
  }

  // -------------------------------------------------------------------------
  // Property 23d: Keywords inside parenthesized expressions are preserved
  // Validates: Requirement 11.3
  // -------------------------------------------------------------------------

  /**
   * For any dialect keyword appearing inside a function call (parenthesized expression),
   * preprocessing SHALL preserve the keyword in that context.
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 23: Token-aware preprocessing preserves"
          + " non-top-level keywords")
  void keywordsInsideFunctionArgsArePreserved(
      @ForAll("keywordCaseVariants") String caseVariant) {
    // Use keyword as a column name inside a function call
    String query = "SELECT func(" + caseVariant + ") FROM tbl";
    String preprocessed = preprocessor.preprocess(query);

    assertTrue(
        preprocessed.contains("func(" + caseVariant + ")"),
        "Keyword '"
            + caseVariant
            + "' inside function args must be preserved. "
            + "Input: '"
            + query
            + "', Output: '"
            + preprocessed
            + "'");
  }

  /**
   * For any dialect keyword appearing inside a subquery (parenthesized expression), preprocessing
   * SHALL preserve the keyword in that context.
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 23: Token-aware preprocessing preserves"
          + " non-top-level keywords")
  void keywordsInsideSubqueriesArePreserved(
      @ForAll("keywordCaseVariants") String caseVariant) {
    // Use keyword as a column alias inside a subquery
    String query =
        "SELECT * FROM (SELECT col1 AS " + caseVariant + " FROM tbl) sub";
    String preprocessed = preprocessor.preprocess(query);

    assertTrue(
        preprocessed.contains("AS " + caseVariant),
        "Keyword '"
            + caseVariant
            + "' inside subquery must be preserved. "
            + "Input: '"
            + query
            + "', Output: '"
            + preprocessed
            + "'");
  }

  // -------------------------------------------------------------------------
  // Property 23e: Mixed case keywords at top level ARE stripped
  // Validates: Requirement 11.4
  // -------------------------------------------------------------------------

  /**
   * For any case variant of FORMAT/SETTINGS/FINAL at top level, preprocessing SHALL strip them.
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 23: Token-aware preprocessing preserves"
          + " non-top-level keywords")
  void mixedCaseTopLevelKeywordsAreStripped(
      @ForAll("baseQueries") String base,
      @ForAll("topLevelClauses") String clause) {
    String query = base + " " + clause;
    String preprocessed = preprocessor.preprocess(query);
    String normalizedPreprocessed = normalizeWhitespace(preprocessed);
    String normalizedBase = normalizeWhitespace(base);

    assertEquals(
        normalizedBase,
        normalizedPreprocessed,
        "Top-level clause '"
            + clause
            + "' should be stripped. "
            + "Input: '"
            + query
            + "', Output: '"
            + preprocessed
            + "'");
  }

  // -------------------------------------------------------------------------
  // Property 23f: Protected keywords preserved while top-level stripped
  // Validates: Requirements 11.1, 11.2, 11.3, 11.4, 11.5
  // -------------------------------------------------------------------------

  /**
   * Combined property: queries with keywords in BOTH protected contexts AND top-level positions
   * SHALL have only the top-level occurrences stripped while protected ones are preserved.
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 23: Token-aware preprocessing preserves"
          + " non-top-level keywords")
  void protectedKeywordsPreservedWhileTopLevelStripped(
      @ForAll("queriesWithProtectedAndTopLevel") Tuple.Tuple2<String, String> queryAndProtected) {
    String query = queryAndProtected.get1();
    String protectedFragment = queryAndProtected.get2();

    String preprocessed = preprocessor.preprocess(query);

    // The protected fragment must survive
    assertTrue(
        preprocessed.contains(protectedFragment),
        "Protected fragment '"
            + protectedFragment
            + "' must be preserved after stripping top-level clauses. "
            + "Input: '"
            + query
            + "', Output: '"
            + preprocessed
            + "'");
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> baseQueries() {
    return Arbitraries.of(
        "SELECT col1 FROM tbl",
        "SELECT col1, col2 FROM tbl WHERE col1 > 0",
        "SELECT col1 FROM tbl ORDER BY col1",
        "SELECT col1 FROM tbl GROUP BY col1",
        "SELECT a, b FROM tbl WHERE a > 10",
        "SELECT MAX(col1) FROM tbl",
        "SELECT col1 FROM tbl LIMIT 100");
  }

  @Provide
  Arbitrary<String> dialectKeywords() {
    return Arbitraries.of("FORMAT", "SETTINGS", "FINAL");
  }

  @Provide
  Arbitrary<String> keywordCaseVariants() {
    return Arbitraries.of(
        "FORMAT", "Format", "format", "FoRmAt",
        "SETTINGS", "Settings", "settings", "sEtTiNgS",
        "FINAL", "Final", "final", "fInAl");
  }

  @Provide
  Arbitrary<String> topLevelClauses() {
    Arbitrary<String> formatClauses =
        Arbitraries.of("Format", "FORMAT", "format", "FoRmAt")
            .flatMap(
                kw ->
                    Arbitraries.of("JSON", "CSV", "TabSeparated", "Pretty")
                        .map(fmt -> kw + " " + fmt));

    Arbitrary<String> settingsClauses =
        Arbitraries.of("SETTINGS", "Settings", "settings", "sEtTiNgS")
            .flatMap(
                kw ->
                    Arbitraries.of("max_threads", "max_memory_usage")
                        .flatMap(
                            key ->
                                Arbitraries.integers()
                                    .between(1, 100)
                                    .map(v -> kw + " " + key + "=" + v)));

    Arbitrary<String> finalClauses =
        Arbitraries.of("FINAL", "Final", "final", "fInAl");

    return Arbitraries.oneOf(formatClauses, settingsClauses, finalClauses);
  }

  @Provide
  Arbitrary<Tuple.Tuple2<String, String>> queriesWithProtectedAndTopLevel() {
    return Arbitraries.of(
        // String literal + top-level FORMAT
        Tuple.of(
            "SELECT 'FORMAT JSON' AS cfg FROM tbl FORMAT CSV",
            "'FORMAT JSON'"),
        // String literal + top-level SETTINGS
        Tuple.of(
            "SELECT col1 FROM tbl WHERE name = 'SETTINGS max_threads=2' SETTINGS max_threads=4",
            "'SETTINGS max_threads=2'"),
        // String literal + top-level FINAL
        Tuple.of(
            "SELECT 'FINAL' AS kw FROM tbl FINAL",
            "'FINAL'"),
        // Block comment + top-level FORMAT
        Tuple.of(
            "SELECT /* FORMAT JSON */ col1 FROM tbl FORMAT TabSeparated",
            "/* FORMAT JSON */"),
        // Block comment + top-level SETTINGS
        Tuple.of(
            "SELECT /* SETTINGS note */ col1 FROM tbl SETTINGS max_threads=2",
            "/* SETTINGS note */"),
        // Line comment + top-level FORMAT (line comment at end absorbs FORMAT)
        Tuple.of(
            "SELECT col1 FROM tbl -- FINAL is here\nORDER BY col1 FORMAT JSON",
            "-- FINAL is here"),
        // Function arg + top-level FORMAT
        Tuple.of(
            "SELECT func(FORMAT) FROM tbl FORMAT JSON",
            "func(FORMAT)"),
        // Subquery + top-level SETTINGS
        Tuple.of(
            "SELECT * FROM (SELECT FINAL FROM tbl) sub SETTINGS max_threads=2",
            "SELECT FINAL FROM tbl"),
        // Nested parens + top-level FINAL
        Tuple.of(
            "SELECT func(inner(SETTINGS)) FROM tbl FINAL",
            "func(inner(SETTINGS))"),
        // Multiple protected contexts + top-level clause
        Tuple.of(
            "SELECT 'FORMAT' AS a, /* FINAL */ col1 FROM tbl FORMAT JSON",
            "'FORMAT'"));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static String normalizeWhitespace(String s) {
    return s.trim().replaceAll("\\s+", " ");
  }
}
