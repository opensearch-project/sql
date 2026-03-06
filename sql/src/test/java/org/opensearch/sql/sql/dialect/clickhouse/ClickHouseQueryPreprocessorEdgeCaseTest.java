/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClickHouseQueryPreprocessor} edge cases. Verifies that the token-aware
 * preprocessor correctly preserves FORMAT, SETTINGS, and FINAL keywords when they appear inside
 * string literals, comments, function arguments, or nested subqueries, while still stripping
 * top-level occurrences.
 *
 * <p>Validates: Requirements 11.1, 11.2, 11.3, 11.4, 11.5
 */
class ClickHouseQueryPreprocessorEdgeCaseTest {

  private final ClickHouseQueryPreprocessor preprocessor = new ClickHouseQueryPreprocessor();

  // -----------------------------------------------------------------------
  // Requirement 11.1: Keywords inside string literals are preserved
  // -----------------------------------------------------------------------

  @Test
  void formatInStringLiteralIsPreserved() {
    String input = "SELECT 'FORMAT' as a FROM t";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT 'FORMAT' as a FROM t", result);
  }

  @Test
  void settingsInStringLiteralIsPreserved() {
    String input = "SELECT 'SETTINGS' as a FROM t";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT 'SETTINGS' as a FROM t", result);
  }

  @Test
  void finalInStringLiteralIsPreserved() {
    String input = "SELECT 'FINAL' as a FROM t";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT 'FINAL' as a FROM t", result);
  }

  // -----------------------------------------------------------------------
  // Requirement 11.2: Keywords inside comments are preserved
  // -----------------------------------------------------------------------

  @Test
  void formatInBlockCommentIsPreserved() {
    String input = "SELECT /* FORMAT JSON */ * FROM t";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT /* FORMAT JSON */ * FROM t", result);
  }

  @Test
  void finalInLineCommentIsPreserved() {
    String input = "SELECT * FROM t -- FINAL";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t -- FINAL", result);
  }

  @Test
  void settingsInBlockCommentIsPreserved() {
    String input = "SELECT /* SETTINGS max_threads=4 */ * FROM t";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT /* SETTINGS max_threads=4 */ * FROM t", result);
  }

  @Test
  void settingsInLineCommentIsPreserved() {
    String input = "SELECT * FROM t -- SETTINGS max_threads=4";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t -- SETTINGS max_threads=4", result);
  }

  // -----------------------------------------------------------------------
  // Requirement 11.3: Keywords in function args / subqueries preserved,
  //                    top-level occurrences stripped
  // -----------------------------------------------------------------------

  @Test
  void formatInFunctionArgPreservedAndTopLevelFormatStripped() {
    String input = "SELECT format(col, 'JSON') FROM t FORMAT TabSeparated";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT format(col, 'JSON') FROM t", result);
  }

  @Test
  void formatInNestedSubqueryIsPreserved() {
    String input = "SELECT * FROM (SELECT format(x, 'CSV') FROM t2) AS sub";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM (SELECT format(x, 'CSV') FROM t2) AS sub", result);
  }

  @Test
  void finalInsideSubqueryIsPreserved() {
    String input = "SELECT * FROM (SELECT FINAL FROM t2) AS sub";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM (SELECT FINAL FROM t2) AS sub", result);
  }

  @Test
  void settingsInsideSubqueryIsPreserved() {
    String input = "SELECT * FROM (SELECT SETTINGS FROM t2) AS sub";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM (SELECT SETTINGS FROM t2) AS sub", result);
  }

  // -----------------------------------------------------------------------
  // Requirement 11.4: Mixed-case keywords handled correctly
  // -----------------------------------------------------------------------

  @Test
  void mixedCaseFormatIsStripped() {
    String input = "SELECT * FROM t Format JSON";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void mixedCaseSettingsIsStripped() {
    String input = "SELECT * FROM t Settings max_threads=4";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void mixedCaseFinalIsStripped() {
    String input = "SELECT * FROM t final";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void upperCaseAllClausesStripped() {
    String input = "SELECT * FROM t FINAL FORMAT JSON SETTINGS max_threads=4";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  // -----------------------------------------------------------------------
  // Requirement 11.5: Multiple clauses in different orders
  // -----------------------------------------------------------------------

  @Test
  void formatThenSettingsStripped() {
    String input = "SELECT * FROM t FORMAT JSON SETTINGS max_threads=4";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void settingsThenFormatStripped() {
    String input = "SELECT * FROM t SETTINGS max_threads=4 FORMAT JSON";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void finalThenFormatThenSettingsStripped() {
    String input = "SELECT * FROM t FINAL FORMAT TabSeparated SETTINGS max_threads=4";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void settingsThenFinalStripped() {
    String input = "SELECT * FROM t SETTINGS max_threads=4 FINAL";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void formatThenFinalStripped() {
    String input = "SELECT * FROM t FORMAT JSON FINAL";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  // -----------------------------------------------------------------------
  // Combined edge cases: mixed contexts
  // -----------------------------------------------------------------------

  @Test
  void stringLiteralAndTopLevelFormatCombined() {
    String input = "SELECT 'FORMAT' as a FROM t FORMAT JSON";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT 'FORMAT' as a FROM t", result);
  }

  @Test
  void blockCommentAndTopLevelFinalCombined() {
    String input = "SELECT /* FINAL */ * FROM t FINAL";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT /* FINAL */ * FROM t", result);
  }

  @Test
  void lineCommentAndTopLevelSettingsCombined() {
    String input = "SELECT * FROM t -- SETTINGS in comment\nSETTINGS max_threads=4";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t -- SETTINGS in comment", result);
  }

  @Test
  void nestedSubqueryFormatAndTopLevelFormatBothHandled() {
    String input =
        "SELECT * FROM (SELECT format(x, 'JSON') FROM t2) AS sub FORMAT TabSeparated";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM (SELECT format(x, 'JSON') FROM t2) AS sub", result);
  }

  @Test
  void multipleSettingsKeyValuePairsStripped() {
    String input = "SELECT * FROM t SETTINGS max_threads=4, max_memory_usage=1000000";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT * FROM t", result);
  }

  @Test
  void queryWithNoDialectClausesUnchanged() {
    String input = "SELECT a, b FROM t WHERE a > 1 ORDER BY b LIMIT 10";
    String result = preprocessor.preprocess(input);
    assertEquals("SELECT a, b FROM t WHERE a > 1 ORDER BY b LIMIT 10", result);
  }
}
