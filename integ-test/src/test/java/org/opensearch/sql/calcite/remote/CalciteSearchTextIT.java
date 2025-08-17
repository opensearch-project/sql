/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for search text functionality in PPL. Tests free text search, boolean
 * operators, phrase search, and mixed queries.
 */
public class CalciteSearchTextIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.PHRASE);
    enableCalcite();
  }

  // ========== Basic Free Text Search Tests ==========

  @Test
  public void testSingleWordSearch() throws IOException {
    // Search for "Holmes" which appears in address field
    JSONObject result =
        executeQuery(String.format("search Holmes source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));
  }

  @Test
  public void testMultipleWordsImplicitAnd() throws IOException {
    // Search for "Bristol Street" - both words must be present (implicit AND)
    JSONObject result =
        executeQuery(
            String.format("search Bristol Street source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"));
  }

  @Test
  public void testPhraseSearchWithQuotes() throws IOException {
    // Search for exact phrase "Kings Place"
    // This should NOT match documents with "Kings" and "Street" (non-adjacent)
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"Kings Place\\\" source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("282 Kings Place"));

    // Verify it doesn't match other addresses with "Street"
    JSONObject wrongResult =
        executeQuery(
            String.format(
                "search \\\"Kings Street\\\" source=%s | fields address", TEST_INDEX_BANK));
    Assert.assertEquals(0, wrongResult.getJSONArray("datarows").length());
  }

  @Test
  public void testPhraseVsWordsSearch() throws IOException {
    // Without quotes: finds documents with both "Bristol" AND "Street" (anywhere)
    JSONObject wordsResult =
        executeQuery(
            String.format("search Bristol Street source=%s | stats count()", TEST_INDEX_BANK));
    int wordsCount = wordsResult.getJSONArray("datarows").getJSONArray(0).getInt(0);
    Assert.assertEquals(1, wordsCount); // Should only match "671 Bristol Street"

    // With quotes: finds exact phrase "Bristol Street"
    JSONObject phraseResult =
        executeQuery(
            String.format(
                "search \\\"Bristol Street\\\" source=%s | stats count()", TEST_INDEX_BANK));
    int phraseCount = phraseResult.getJSONArray("datarows").getJSONArray(0).getInt(0);
    Assert.assertEquals(1, phraseCount); // Should match "671 Bristol Street"

    // With quotes but wrong order: should NOT match
    JSONObject wrongOrderResult =
        executeQuery(
            String.format(
                "search \\\"Street Bristol\\\" source=%s | stats count()", TEST_INDEX_BANK));
    int wrongOrderCount = wrongOrderResult.getJSONArray("datarows").getJSONArray(0).getInt(0);
    Assert.assertEquals(0, wrongOrderCount); // Should not match anything
  }

  @Test
  public void testNumericLiteralSearch() throws IOException {
    // Test searching with numeric literals - should find "671 Bristol Street"
    JSONObject result =
        executeQuery(
            String.format("search 671 Bristol source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"));

    // Test with only numeric
    JSONObject numericOnlyResult =
        executeQuery(String.format("search 671 source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(numericOnlyResult, rows("671 Bristol Street"));

    // Test numeric in phrase
    JSONObject phraseWithNumeric =
        executeQuery(
            String.format(
                "search \\\"671 Bristol\\\" source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(phraseWithNumeric, rows("671 Bristol Street"));
  }

  @Test
  public void testSearchAcrossMultipleFields() throws IOException {
    // "Pyrami" appears in employer field
    JSONObject result =
        executeQuery(
            String.format("search Pyrami source=%s | fields firstname, employer", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny", "Pyrami"));
  }

  // ========== Boolean Operators Tests ==========

  @Test
  public void testAndOperator() throws IOException {
    // Search for documents with both "Holmes" AND "Lane"
    JSONObject result =
        executeQuery(
            String.format("search Holmes AND Lane source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));
  }

  @Test
  public void testOrOperator() throws IOException {
    // Search for documents with either "Holmes" OR "Bristol"
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes OR Bristol source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"), rows("880 Holmes Lane"));
  }

  @Test
  public void testNotOperator() throws IOException {
    // Search for "Street" but NOT "Bristol"
    JSONObject result =
        executeQuery(
            String.format(
                "search Street AND NOT Bristol source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"), rows("702 Quentin Street"));
  }

  @Test
  public void testOperatorPrecedenceNotOverAnd() throws IOException {
    // NOT has higher precedence than AND
    // "Street AND NOT Bristol" should parse as "Street AND (NOT Bristol)"
    JSONObject result =
        executeQuery(
            String.format(
                "search Street AND NOT Bristol source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"), rows("702 Quentin Street"));
  }

  @Test
  public void testOperatorPrecedenceAndOverOr() throws IOException {
    // AND has higher precedence than OR
    // "Holmes AND Lane OR Madison" should parse as "(Holmes AND Lane) OR Madison"
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes AND Lane OR Madison source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street"), rows("880 Holmes Lane"));
  }

  @Test
  public void testParenthesesForGrouping() throws IOException {
    // Use parentheses to override precedence
    // "(Bristol OR Madison) AND Street" - both streets contain "Street"
    JSONObject result =
        executeQuery(
            String.format(
                "search (Bristol OR Madison) AND Street source=%s | fields address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"), rows("789 Madison Street"));
  }

  @Test
  public void testComplexBooleanExpression() throws IOException {
    // Complex: (Street AND NOT Bristol) OR (Court AND Hutchinson)
    JSONObject result =
        executeQuery(
            String.format(
                "search (Street AND NOT Bristol) OR (Court AND Hutchinson) source=%s | fields"
                    + " address | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows("467 Hutchinson Court"),
        rows("789 Madison Street"),
        rows("702 Quentin Street"));
  }

  // ========== Mixed Queries (Text + Field Comparisons) ==========

  @Test
  public void testFreeTextWithFieldEquality() throws IOException {
    // Search for "Street" and filter by age
    JSONObject result =
        executeQuery(
            String.format(
                "search Street age=36 source=%s | fields address, age | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street", 36));
  }

  @Test
  public void testFreeTextWithFieldComparison() throws IOException {
    // Search for "Street" with age greater than 30
    // Should return Bristol Street (age 36) and Quentin Street (age 34)
    JSONObject result =
        executeQuery(
            String.format(
                "search Street age > 30 source=%s | fields address, age | sort address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street", 36), rows("702 Quentin Street", 34));
  }

  @Test
  public void testBooleanTextWithFieldComparison() throws IOException {
    // Boolean text search combined with field comparison
    JSONObject result =
        executeQuery(
            String.format(
                "search (Holmes OR Madison) AND age < 35 source=%s | fields address, age | sort"
                    + " address",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("789 Madison Street", 28), rows("880 Holmes Lane", 32));
  }

  @Test
  public void testComplexMixedQuery() throws IOException {
    // Complex mix: (text OR text) AND field=value AND field>value
    JSONObject result =
        executeQuery(
            String.format(
                "search (Pyrami OR Netagy) gender=\\\"M\\\" age > 30 source=%s | fields employer,"
                    + " gender, age | sort employer",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Netagy", "M", 36), rows("Pyrami", "M", 32));
  }

  // ========== Phrase Search Tests ==========

  @Test
  public void testExactPhraseMatch() throws IOException {
    // Test exact phrase matching in phrase index
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"quick fox\\\" source=%s | fields phrase | sort phrase",
                TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void testPhraseOrderMatters() throws IOException {
    // "fox brown" should not match "brown fox"
    JSONObject result =
        executeQuery(
            String.format("search \\\"brown fox\\\" source=%s | fields phrase", TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("brown fox"));
  }

  @Test
  public void testPhrasesWithBooleanOperators() throws IOException {
    // Combine phrases with OR
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"quick fox\\\" OR \\\"brown fox\\\" source=%s | fields phrase | sort"
                    + " phrase",
                TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("brown fox"), rows("quick fox"), rows("quick fox here"));
  }

  // ========== Wildcard Search Tests ==========

  @Test
  public void testWildcardSearchSingleAsterisk() throws IOException {
    // Search with wildcard * (matches zero or more characters)
    JSONObject result =
        executeQuery(
            String.format(
                "search Py*mi source=%s | fields employer | sort employer", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Pyrami"));
  }

  @Test
  public void testWildcardSearchWithStar() throws IOException {
    // Search with wildcard * in place of single character
    // Looking for "Dale" with D*le pattern
    JSONObject result =
        executeQuery(String.format("search D*le source=%s | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Dale"));
  }

  @Test
  public void testWildcardSearchMultipleAsterisks() throws IOException {
    // Search with multiple wildcards
    // H*tt* should match "Hattie"
    JSONObject result =
        executeQuery(String.format("search H*tt* source=%s | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testWildcardSearchInMiddle() throws IOException {
    // Search with wildcard in the middle of word
    // Br*gan should match "Brogan" (city name)
    JSONObject result =
        executeQuery(String.format("search Br*gan source=%s | fields city", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Brogan"));
  }

  @Test
  public void testWildcardSearchAtBeginning() throws IOException {
    // Search with wildcard at the beginning
    // *liff should match "Ratliff"
    JSONObject result =
        executeQuery(String.format("search *liff source=%s | fields lastname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Ratliff"));
  }

  @Test
  public void testWildcardSearchAtEnd() throws IOException {
    // Search with wildcard at the end
    // Net* should match "Netagy"
    JSONObject result =
        executeQuery(String.format("search Net* source=%s | fields employer", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Netagy"));
  }

  @Test
  public void testWildcardWithBooleanOperators() throws IOException {
    // Combine wildcards with boolean operators
    // Search for Py* OR Net*
    JSONObject result =
        executeQuery(
            String.format(
                "search Py* OR Net* source=%s | fields employer | sort employer", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Netagy"), rows("Pyrami"));
  }

  @Test
  public void testWildcardInPhrase() throws IOException {
    // Wildcards in quoted phrases should be treated literally (no wildcard expansion)
    // This should NOT match anything as there's no literal "H*lmes" in the data
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"H*lmes Lane\\\" source=%s | fields address", TEST_INDEX_BANK));
    Assert.assertEquals(0, result.getJSONArray("datarows").length());
  }

  @Test
  public void testWildcardWithFieldComparison() throws IOException {
    // Combine wildcard search with field comparison
    JSONObject result =
        executeQuery(
            String.format("search Py* age=32 source=%s | fields employer, age", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Pyrami", 32));
  }

  // ========== Special Characters and Edge Cases ==========

  @Test
  public void testEmailAddressSearch() throws IOException {
    // Search for email addresses (contains special characters)
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"amberduke@pyrami.com\\\" source=%s | fields email", TEST_INDEX_BANK));
    verifyDataRows(result, rows("amberduke@pyrami.com"));
  }

  @Test
  public void testHyphenatedSearch() throws IOException {
    // Search for hyphenated terms
    JSONObject result =
        executeQuery(
            String.format(
                "search \\\"Amber JOHnny\\\" source=%s | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny"));
  }

  @Test
  public void testNumbersInText() throws IOException {
    // Search for addresses with numbers
    JSONObject result =
        executeQuery(
            String.format("search \\\"880 Holmes\\\" source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));
  }

  @Test
  public void testEmptySearchWithFieldFilter() throws IOException {
    // Empty search text with field filter should work
    JSONObject result =
        executeQuery(
            String.format("search age=32 source=%s | fields firstname, age", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny", 32));
  }

  // ========== Optimization Verification Tests ==========

  @Test
  public void testAdjacentTextOptimization() throws IOException {
    // Multiple adjacent text terms should be combined into single query_string
    // This is a functional test - the optimization happens internally
    JSONObject result =
        executeQuery(
            String.format(
                "search Bristol Street Bristol source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street"));
  }

  @Test
  public void testAndChainOptimization() throws IOException {
    // Chain of AND operations should be optimized
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes AND Lane AND 880 source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));
  }

  @Test
  public void testOrChainOptimization() throws IOException {
    // Chain of OR operations should be optimized
    JSONObject result =
        executeQuery(
            String.format(
                "search Pyrami OR Netagy OR Quility source=%s | fields employer | sort employer",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Netagy"), rows("Pyrami"), rows("Quility"));
  }

  // ========== Case Sensitivity Tests ==========

  @Test
  public void testCaseInsensitiveSearch() throws IOException {
    // Search in text fields (like address) is case-insensitive
    JSONObject result =
        executeQuery(String.format("search holmes source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane"));

    // Also works with uppercase
    JSONObject upperResult =
        executeQuery(String.format("search HOLMES source=%s | fields address", TEST_INDEX_BANK));
    verifyDataRows(upperResult, rows("880 Holmes Lane"));
  }

  @Test
  public void testMixedCaseInData() throws IOException {
    // For keyword fields, search needs to match the ENTIRE field value exactly
    // "JOHnny" alone won't match "Amber JOHnny" in a keyword field
    JSONObject partialResult =
        executeQuery(String.format("search JOHnny source=%s | stats count()", TEST_INDEX_BANK));
    int partialCount = partialResult.getJSONArray("datarows").getJSONArray(0).getInt(0);
    Assert.assertEquals(0, partialCount);

    // To match, we need the exact full value with quotes
    JSONObject exactResult =
        executeQuery(
            String.format(
                "search \\\"Amber JOHnny\\\" source=%s | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(exactResult, rows("Amber JOHnny"));

    // But searching in text fields (like email) is case-insensitive and partial
    JSONObject textFieldResult =
        executeQuery(String.format("search amberduke source=%s | fields email", TEST_INDEX_BANK));
    verifyDataRows(textFieldResult, rows("amberduke@pyrami.com"));
  }

  // ========== Complex Real-World Scenarios ==========

  @Test
  public void testSearchWithMultipleFiltersAndSort() throws IOException {
    // Real-world scenario: search text, filter by multiple fields, sort results
    JSONObject result =
        executeQuery(
            String.format(
                "search Street gender=\\\"M\\\" age > 30 source=%s | fields address, firstname, age"
                    + " | sort age",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("671 Bristol Street", "Hattie", 36));
  }

  @Test
  public void testSearchWithAggregation() throws IOException {
    // Search combined with aggregation
    JSONObject result =
        executeQuery(
            String.format("search Street source=%s | stats count() by gender", TEST_INDEX_BANK));
    // Verify we get aggregation results
    Assert.assertTrue(result.has("datarows"));
  }

  @Test
  public void testSearchWithProjection() throws IOException {
    // Search with specific field projection
    JSONObject result =
        executeQuery(
            String.format(
                "search Holmes source=%s | fields address, age | head 1", TEST_INDEX_BANK));
    verifyDataRows(result, rows("880 Holmes Lane", 32));
  }

  // ========== Error Cases ==========

  @Test
  public void testInvalidOperatorCombination() throws IOException {
    // Test that invalid operator combinations are handled gracefully
    try {
      executeQuery(String.format("search AND OR source=%s", TEST_INDEX_BANK));
      Assert.fail("Expected query to fail with invalid operator combination");
    } catch (Exception e) {
      // After implementation, this should give a specific error about invalid syntax
      // For now, just verify it fails
      String message = e.getMessage();
      Assert.assertTrue(
          "Expected syntax error but got: " + message,
          message.contains("SyntaxCheckException")
              || message.contains("ParsingException")
              || message.contains("Failed to parse")
              || message.contains("Error occurred"));
    }
  }

  @Test
  public void testUnmatchedParentheses() throws IOException {
    // Test unmatched parentheses
    try {
      executeQuery(String.format("search (Holmes OR Bristol source=%s", TEST_INDEX_BANK));
      Assert.fail("Expected query to fail with unmatched parentheses");
    } catch (Exception e) {
      // After implementation, this should give a specific error about unmatched parentheses
      String message = e.getMessage();
      Assert.assertTrue(
          "Expected syntax error but got: " + message,
          message.contains("SyntaxCheckException")
              || message.contains("ParsingException")
              || message.contains("mismatched")
              || message.contains("Error occurred"));
    }
  }
}
