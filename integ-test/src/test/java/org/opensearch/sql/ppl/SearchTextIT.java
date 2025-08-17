/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for search text functionality in PPL (Legacy Engine). This complements
 * CalciteSearchTextIT for non-Calcite execution paths.
 */
public class SearchTextIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.PHRASE);
    // Note: Not enabling Calcite - this tests the legacy engine path
  }

  // ========== Basic Compatibility Tests ==========
  // These ensure the search functionality works with both engines

  @Test
  public void testBasicTextSearch() throws IOException {
    JSONObject result =
        executeQuery(String.format("search Holmes source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));
  }

  @Test
  public void testPhraseSearch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"Madison Street\\\" source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"));
  }

  @Test
  public void testBooleanOperators() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes OR Bristol source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"), rows("880 Holmes Lane"));
  }

  @Test
  public void testMixedQueries() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search Street age > 30 source=%s | fields address, age | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street", 36), rows("702 Quentin Street", 34));
  }

  @Test
  public void testComplexQuery() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search (Bristol OR Madison) AND Street source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"), rows("789 Madison Street"));
  }

  // ========== Explain Query Tests ==========
  // Verify the query structure and optimization

  @Test
  public void testExplainSimpleSearch() throws IOException {
    String explain =
        explainQueryToString(String.format("search Holmes source=%s", TEST_INDEX_BANK));

    // Verify that query_string function is used (after implementation)
    assertTrue(explain.contains("query_string"));
  }

  @Test
  public void testExplainBooleanSearch() throws IOException {
    String explain =
        explainQueryToString(String.format("search Holmes AND Lane source=%s", TEST_INDEX_BANK));

    // Verify query_string is used with boolean logic preserved
    assertTrue(explain.contains("query_string"));
  }

  @Test
  public void testExplainMixedSearch() throws IOException {
    String explain =
        explainQueryToString(String.format("search Holmes age=32 source=%s", TEST_INDEX_BANK));

    // Should contain query_string for text search and field comparison
    assertTrue(explain.contains("query_string"));
    assertTrue(explain.contains("age") || explain.contains("32"));
  }

  // ========== Performance-Related Tests ==========

  @Test
  public void testLargeTextQuery() throws IOException {
    // Test with multiple terms that should be optimized
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes Lane Street Bristol Madison source=%s | stats count()",
                TEST_INDEX_BANK));

    // Just verify it executes without error
    assertTrue(result.has("datarows"));
  }

  @Test
  public void testMultipleAndConditions() throws IOException {
    // Multiple AND conditions should be optimized into single query
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes AND Lane AND 880 source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));
  }

  @Test
  public void testMultipleOrConditions() throws IOException {
    // Multiple OR conditions should be optimized
    JSONObject result =
        executeQuery(
            String.format(
                "search Pyrami OR Netagy OR Quility source=%s | stats count()", TEST_INDEX_BANK));

    // Verify we get 3 matches
    int count = result.getJSONArray("datarows").getJSONArray(0).getInt(0);
    assertEquals(3, count);
  }

  // ========== Complex Boolean Logic Tests ==========

  @Test
  public void testComplexGroupingWithNot() throws IOException {
    // Test: NOT (Holmes OR Bristol) - should exclude both
    JSONObject result =
        executeQuery(
            String.format(
                "search NOT (Holmes OR Bristol) source=%s | head 5 | fields address",
                TEST_INDEX_BANK));

    // Verify no results contain Holmes or Bristol
    JSONArray rows = result.getJSONArray("datarows");
    for (int i = 0; i < rows.length(); i++) {
      String address = rows.getJSONArray(i).getString(0);
      assertFalse(address.contains("Holmes"));
      assertFalse(address.contains("Bristol"));
    }
  }

  @Test
  public void testExpressionsBeforeAndAfterSource() throws IOException {
    // Search text before source and field comparison after
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes source=%s age>=32 | fields firstname, age, address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny", 32, "880 Holmes Lane"));
  }

  @Test
  public void testMultipleNotWithGrouping() throws IOException {
    // Multiple NOT operators: NOT Holmes AND NOT (Bristol OR Pyrami)
    JSONObject result =
        executeQuery(
            String.format(
                "search NOT Holmes AND NOT (Bristol OR Pyrami) source=%s | fields address, email |"
                    + " head 3",
                TEST_INDEX_BANK));

    // Verify none of the results contain Holmes, Bristol, or Pyrami
    JSONArray rows = result.getJSONArray("datarows");
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      String address = row.getString(0);
      String email = row.getString(1);
      assertFalse(address.contains("Holmes"));
      assertFalse(address.contains("Bristol"));
      assertFalse(email.contains("pyrami"));
    }
  }

  @Test
  public void testDeepNestedGroups() throws IOException {
    // Deep nesting: ((880 OR 671) AND (Lane OR Street))
    JSONObject result =
        executeQuery(
            String.format(
                "search ((880 OR 671) AND (Lane OR Street)) source=%s | fields address | sort"
                    + " address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"), rows("880 Holmes Lane"));
  }

  @Test
  public void testMixedTextFieldsAndGrouping() throws IOException {
    // Mix of search text with grouping before source, field comparisons after
    JSONObject result =
        executeQuery(
            String.format(
                "search (Street OR Lane) AND NOT Bristol source=%s gender=\\\"M\\\" age>=30 |"
                    + " fields firstname, address | sort firstname",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny", "880 Holmes Lane"));
  }

  // ========== Quoted Phrases and Special Characters ==========

  @Test
  public void testQuotedPhrasesWithGrouping() throws IOException {
    // Quoted phrases with grouping and NOT operators
    JSONObject result =
        executeQuery(
            String.format(
                "search (\\\"Holmes Lane\\\" OR \\\"Madison Street\\\") AND NOT Bristol source=%s |"
                    + " fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"), rows("880 Holmes Lane"));
  }

  @Test
  public void testNumericAndTextMixedGrouping() throws IOException {
    // Mix of numeric and text values with complex grouping
    JSONObject result =
        executeQuery(
            String.format(
                "search (880 OR 789) AND (Street OR Lane) source=%s | fields address | sort"
                    + " address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"), rows("880 Holmes Lane"));
  }

  @Test
  public void testWildcardsWithGrouping() throws IOException {
    // Wildcards with grouping and boolean operators
    JSONObject result =
        executeQuery(
            String.format(
                "search (Bri* OR Mad*) AND Street source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"), rows("671 Bristol Street"));
  }

  @Test
  public void testSpecialCharsInQuotes() throws IOException {
    // Special characters should work in quoted phrases
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"amberduke@pyrami.com\\\" source=%s | fields firstname, email",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny", "amberduke@pyrami.com"));
  }

  // ========== Edge Cases and Complex Scenarios ==========

  @Test
  public void testNumericLiteralsInSearch() throws IOException {
    // Numeric literals should be treated as search text
    JSONObject result =
        executeQuery(
            String.format("search 671 AND Bristol source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"));
  }

  @Test
  public void testMultipleWordsImplicitAnd() throws IOException {
    // Multiple words without operators should use implicit AND
    JSONObject result =
        executeQuery(
            String.format("search Holmes Lane 880 source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));
  }

  @Test
  public void testCaseSensitivity() throws IOException {
    // Test that search is case insensitive for text fields
    JSONObject result =
        executeQuery(String.format("search holmes source=%s | fields address", TEST_INDEX_BANK));
    System.out.println(result.toString());
    assertEquals(1, result.getJSONArray("datarows").length());
  }

  @Test
  public void testEmptySearch() throws IOException {
    // Empty search should return all documents
    JSONObject result =
        executeQuery(String.format("search source=%s | head 5 | stats count()", TEST_INDEX_BANK));
    int count = result.getJSONArray("datarows").getJSONArray(0).getInt(0);
    assertEquals(5, count);
  }

  @Test
  public void testSearchWithOnlyFieldComparison() throws IOException {
    // Only field comparison, no free text
    JSONObject result =
        executeQuery(
            String.format("search age=32 source=%s | fields firstname, age", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny", 32));
  }

  @Test
  public void testComplexNestedExpression() throws IOException {
    // Test the specific case from user: (671 AND NOT "Bristol") OR (Madison AND 789)
    JSONObject result =
        executeQuery(
            String.format(
                "search (671 AND NOT \\\"Bristol\\\") OR (Madison AND 789) source=%s | fields"
                    + " address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"));
  }

  @Test
  public void testOperatorPrecedence() throws IOException {
    // Test that NOT has higher precedence than AND/OR
    // NOT Holmes OR Bristol means (NOT Holmes) OR Bristol
    JSONObject result =
        executeQuery(
            String.format(
                "search NOT Holmes OR Bristol source=%s | fields address | sort address",
                TEST_INDEX_BANK));

    // Should include Bristol Street and exclude Holmes Lane
    boolean hasBristol = false;
    boolean hasHolmes = false;
    JSONArray rows = result.getJSONArray("datarows");
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      String address = row.getString(0);
      if (address.contains("Bristol")) hasBristol = true;
      if (address.contains("Holmes")) hasHolmes = true;
    }
    assertTrue(hasBristol);
    assertFalse(hasHolmes);
  }
}
