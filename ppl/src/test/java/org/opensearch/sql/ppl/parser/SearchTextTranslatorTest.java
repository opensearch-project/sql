/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.*;

import org.junit.Test;

public class SearchTextTranslatorTest extends AstBuilderTest {

  @Test
  public void testSearchWithFreeText() {
    assertEqual(
        "search hello world source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(hello AND world)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithQuotedPhrase() {
    assertEqual(
        "search \"my phrase\" source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("\"my phrase\"")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithOrOperator() {
    assertEqual(
        "search hello OR world source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(hello OR world)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithAndOperator() {
    assertEqual(
        "search hello AND world source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(hello AND world)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithNoOperator() {
    assertEqual(
        "search hello source=index world",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(hello AND world)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithNotOperator() {
    assertEqual(
        "search NOT hello source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("NOT hello")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithMixedTextAndFieldComparison() {
    assertEqual(
        "search hello world field=value source=index",
        filter(
            relation("index"),
            and(
                function(
                    "query_string",
                    unresolvedArg("query", stringLiteral("(hello AND world)")),
                    unresolvedArg("default_operator", stringLiteral("AND"))),
                compare("=", field("field"), field("value")))));
  }

  @Test
  public void testSearchWithComplexBooleanLogic() {
    // (hello OR world) AND test - entire expression is search text, so single query_string
    assertEqual(
        "search (hello OR world) AND test source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("((hello OR world) AND test)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithOrAndFieldComparison() {
    // Search text with OR operator AND field comparison (implicit AND between them)
    assertEqual(
        "search hello OR world field=value source=index",
        filter(
            relation("index"),
            and(
                function(
                    "query_string",
                    unresolvedArg("query", stringLiteral("(hello OR world)")),
                    unresolvedArg("default_operator", stringLiteral("AND"))),
                compare("=", field("field"), field("value")))));
  }

  @Test
  public void testSearchWithMultipleWords() {
    // Multiple words are combined with implicit AND
    assertEqual(
        "search quick brown fox source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(quick AND brown AND fox)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithParentheses() {
    // Test proper precedence with parentheses - entire expression is search text
    assertEqual(
        "search test AND (hello OR world) source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(test AND (hello OR world))")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithNotAndOr() {
    // NOT has higher precedence than OR, so this becomes (NOT hello) OR world
    // Entire expression is search text, so single query_string
    assertEqual(
        "search NOT hello OR world source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(NOT hello OR world)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchOptimizesAndChain() {
    // Multiple AND operators are optimized into single query_string
    assertEqual(
        "search hello AND world AND test source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(hello AND world AND test)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchOptimizesOrChain() {
    // Multiple OR operators are optimized into single query_string
    assertEqual(
        "search hello OR world OR test source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(hello OR world OR test)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithWildcard() {
    // Wildcard search - wildcards are passed through to query_string
    assertEqual(
        "search He*llo source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("He*llo")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithMultipleWildcards() {
    // Multiple wildcards in search - using only * wildcard
    assertEqual(
        "search H*llo W*rld source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(H*llo AND W*rld)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithWildcardAndBoolean() {
    // Wildcard with boolean operators - using only * wildcard to avoid ? parsing issues
    assertEqual(
        "search He*llo OR W*rld source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(He*llo OR W*rld)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithNumericLiteral() {
    // Numeric literals should be treated as search text
    assertEqual(
        "search 671 Bristol source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(671 AND Bristol)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithNumericAndString() {
    // Mix of numeric and string literals
    assertEqual(
        "search 123 test 456 source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(123 AND test AND 456)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithEmptySearch() {
    // Empty search returns just the source
    assertEqual("search source=index", relation("index"));
  }

  @Test
  public void testSearchWithOnlyFieldComparison() {
    // Only field comparison, no free text
    assertEqual(
        "search field=value source=index",
        filter(relation("index"), compare("=", field("field"), field("value"))));
  }

  @Test
  public void testSearchWithComplexNestedExpression() {
    // Test the specific case: (671 AND NOT "Bristol") OR (Court AND Hutchinson)
    // Note: The quoted phrase "Bristol" becomes Bristol in the query_string
    assertEqual(
        "search (671 AND NOT \"Bristol\") OR (Court AND Hutchinson) source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg(
                    "query", stringLiteral("((671 AND NOT Bristol) OR (Court AND Hutchinson))")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithComplexGroupingAndNot() {
    // Complex grouping with NOT on multiple levels
    assertEqual(
        "search NOT (hello OR world) AND (test OR demo) source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(NOT (hello OR world) AND (test OR demo))")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  /** The precedence order here is not followed and should be solved later.* */
  @Test
  public void testSearchWithExpressionsBeforeAndAfterSource() {
    // Search text before source and field comparison after
    assertEqual(
        "search (quick AND brown) OR fox source=index NOT (slow OR lazy) field=value",
        filter(
            relation("index"),
            and(
                function(
                    "query_string",
                    unresolvedArg(
                        "query",
                        stringLiteral("(((quick AND brown) OR fox) AND NOT (slow OR lazy))")),
                    unresolvedArg("default_operator", stringLiteral("AND"))),
                compare("=", field("field"), field("value")))));
  }

  @Test
  public void testSearchWithMultipleNotAndGrouping() {
    // Multiple NOT operators with complex grouping
    assertEqual(
        "search NOT hello AND NOT (world OR test) AND (demo AND NOT example) source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg(
                    "query",
                    stringLiteral("(NOT hello AND NOT (world OR test) AND demo AND NOT example)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithDeepNestedGroups() {
    // Deep nested groups with mixed operators
    assertEqual(
        "search ((hello OR world) AND (test OR demo)) OR (NOT (foo AND bar)) source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg(
                    "query",
                    stringLiteral("(((hello OR world) AND (test OR demo)) OR NOT (foo AND bar))")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithMixedTextFieldsAndGrouping() {
    // Mix of search text with grouping before source, field comparisons after
    assertEqual(
        "search (error OR warning) AND NOT debug source=index level>=3 category=system",
        filter(
            relation("index"),
            and(
                and(
                    function(
                        "query_string",
                        unresolvedArg("query", stringLiteral("((error OR warning) AND NOT debug)")),
                        unresolvedArg("default_operator", stringLiteral("AND"))),
                    compare(">=", field("level"), intLiteral(3))),
                compare("=", field("category"), field("system")))));
  }

  @Test
  public void testSearchWithQuotedPhrasesAndGrouping() {
    // Quoted phrases with grouping and NOT operators
    // Note: Quoted phrases preserve their quotes in query_string
    assertEqual(
        "search (\"exact phrase\" OR \"another phrase\") AND NOT (\"exclude this\" AND word)"
            + " source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg(
                    "query",
                    stringLiteral(
                        "((\"exact phrase\" OR \"another phrase\") AND NOT (\"exclude this\" AND"
                            + " word))")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithNumericAndTextMixedGrouping() {
    // Mix of numeric and text values with complex grouping
    assertEqual(
        "search (404 OR 500) AND (\"error message\" OR warning) NOT (200 AND success) source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg(
                    "query",
                    stringLiteral(
                        "((404 OR 500) AND (\"error message\" OR warning) AND NOT (200 AND"
                            + " success))")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithWildcardsAndGrouping() {
    // Wildcards with grouping and boolean operators
    assertEqual(
        "search (err* OR warn*) AND NOT (deb* OR inf*) source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("((err* OR warn*) AND NOT (deb* OR inf*))")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithReservedCharactersEscaped() {
    // Test that reserved characters are properly escaped in single words
    assertEqual(
        "search server-name source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("server\\-name")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithSpecialCharsInPhrase() {
    // Special characters in quoted phrases should be escaped
    assertEqual(
        "search \"price > 100\" source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("\"price \\> 100\"")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchWithMathOperators() {
    // Math operators should be escaped when used as literals in phrases
    // Note: The quoted phrase "2+2=4" has its special chars escaped
    assertEqual(
        "search \"2+2=4\" source=index",
        filter(
            relation("index"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("\"2\\+2\\=4\"")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }
}
