/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.When;

/**
 * Comprehensive test suite for search text functionality, including: - Search context management
 * (when it's enabled/disabled) - Multi-part identifier handling (with dots) - Free text vs field
 * reference distinction
 *
 * <p>This consolidates tests from: - AstBuilderSearchContextTest - SearchContextVerificationTest -
 * MultiPartIdentifierSearchTest
 */
public class SearchTextComprehensiveTest extends AstBuilderTest {

  // ========================================================================
  // Tests for contexts where search context should be DISABLED
  // ========================================================================

  @Test
  public void testFunctionArgumentsNotTreatedAsSearchText() {
    // Function arguments should NOT be wrapped in FreeTextExpression
    assertEqual(
        "source=t like(field_a, 'pattern')",
        filter(relation("t"), function("like", field("field_a"), stringLiteral("pattern"))));
  }

  @Test
  public void testNestedFunctionArgumentsNotTreatedAsSearchText() {
    // Nested function arguments should also NOT be wrapped
    assertEqual(
        "source=t concat(upper(field_a), lower(field_b))",
        filter(
            relation("t"),
            function(
                "concat",
                function("upper", field("field_a")),
                function("lower", field("field_b")))));
  }

  @Test
  public void testLambdaBodyNotTreatedAsSearchText() {
    // Lambda body expressions should NOT be wrapped in FreeTextExpression
    assertEqual(
        "source=t filter(array_field, x -> x > 5)",
        filter(
            relation("t"),
            function(
                "filter",
                field("array_field"),
                new LambdaFunction(
                    compare(">", field("x"), intLiteral(5)),
                    Collections.singletonList(qualifiedName("x"))))));
  }

  @Test
  public void testComplexLambdaNotTreatedAsSearchText() {
    // Complex lambda with multiple parameters
    assertEqual(
        "source=t reduce(array_field, 0, (acc, x) -> acc + x)",
        filter(
            relation("t"),
            function(
                "reduce",
                field("array_field"),
                intLiteral(0),
                new LambdaFunction(
                    function("+", field("acc"), field("x")),
                    Arrays.asList(qualifiedName("acc"), qualifiedName("x"))))));
  }

  @Test
  public void testCaseConditionsNotTreatedAsSearchText() {
    // CASE conditions should NOT be wrapped in FreeTextExpression
    assertEqual(
        "source=t field_a = case(age > 18, 'adult', age <= 18, 'child')",
        filter(
            relation("t"),
            compare(
                "=",
                field("field_a"),
                new Case(
                    null,
                    Arrays.asList(
                        new When(
                            compare(">", field("age"), intLiteral(18)), stringLiteral("adult")),
                        new When(
                            compare("<=", field("age"), intLiteral(18)), stringLiteral("child"))),
                    Optional.empty()))));
  }

  @Test
  public void testCastExpressionNotTreatedAsSearchText() {
    // CAST expressions should NOT be wrapped in FreeTextExpression
    assertEqual(
        "source=t cast(field_a as INT) > 100",
        filter(
            relation("t"),
            compare(">", cast(field("field_a"), stringLiteral("INT")), intLiteral(100))));
  }

  @Test
  public void testWhereClauseDoesNotUseSearchContext() {
    // In WHERE clause, 'error' should be treated as a field name, not free text
    assertEqual("source=logs | where error", filter(relation("logs"), field("error")));
  }

  @Test
  public void testWhereClauseWithComparison() {
    // Field comparisons in WHERE should work normally
    assertEqual(
        "source=logs | where status > 400",
        filter(relation("logs"), compare(">", field("status"), intLiteral(400))));
  }

  @Test
  public void testEvalClauseDoesNotUseSearchContext() {
    // In EVAL, field references should NOT be treated as search text
    assertEqual(
        "source=logs | eval new_field = old_field + 10",
        eval(
            relation("logs"),
            let(field("new_field"), function("+", field("old_field"), intLiteral(10)))));
  }

  @Test
  public void testInExpressionDoesNotUseSearchContext() {
    // IN expressions should treat identifiers as field names
    assertEqual(
        "source=logs | where status IN ('error', 'warning')",
        filter(
            relation("logs"),
            new In(
                field("status"), Arrays.asList(stringLiteral("error"), stringLiteral("warning")))));
  }

  @Test
  public void testBetweenDoesNotUseSearchContext() {
    // BETWEEN expressions should treat identifiers as field names
    assertEqual(
        "source=logs | where timestamp BETWEEN start_time AND end_time",
        filter(
            relation("logs"),
            new Between(field("timestamp"), field("start_time"), field("end_time"))));
  }

  @Test
  public void testRelevanceFunctionArgumentsNotTreatedAsSearchText() {
    // Relevance function arguments should not be wrapped
    assertEqual(
        "source=t match(field_a, 'search text')",
        filter(
            relation("t"),
            function(
                "match",
                unresolvedArg("field", qualifiedName("field_a")),
                unresolvedArg("query", stringLiteral("search text")))));
  }

  @Test
  public void testPositionFunctionArgumentsNotTreatedAsSearchText() {
    // Position function with IN keyword - arguments should not be wrapped
    assertEqual(
        "source=t position('substring' IN field_a)",
        filter(relation("t"), function("position", stringLiteral("substring"), field("field_a"))));
  }

  // ========================================================================
  // Tests for multi-part identifier handling (with dots)
  // ========================================================================

  @Test
  public void testMultiPartIdentifierAsSearchText() {
    // Standalone multi-part identifier should be treated as search text
    assertEqual(
        "search response.time source=logs",
        filter(
            relation("logs"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("response.time")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testMultiPartIdentifierInComparison() {
    // Multi-part identifier in comparison should be a field reference
    assertEqual(
        "search response.time > 1000 source=logs",
        filter(
            relation("logs"),
            compare(">", field(qualifiedName("response", "time")), intLiteral(1000))));
  }

  @Test
  public void testMixedMultiPartAndComparison() {
    // Mix of standalone multi-part (search text) and comparison (field reference)
    assertEqual(
        "search user.name AND status.code = 200 source=logs",
        filter(
            relation("logs"),
            and(
                function(
                    "query_string",
                    unresolvedArg("query", stringLiteral("user.name")),
                    unresolvedArg("default_operator", stringLiteral("AND"))),
                compare("=", field(qualifiedName("status", "code")), intLiteral(200)))));
  }

  @Test
  public void testMultipleMultiPartIdentifiers() {
    // Multiple standalone multi-part identifiers
    assertEqual(
        "search user.name OR response.time source=logs",
        filter(
            relation("logs"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(user.name OR response.time)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testDeepMultiPartIdentifier() {
    // Deep nesting with multiple dots
    assertEqual(
        "search user.profile.address.city source=logs",
        filter(
            relation("logs"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("user.profile.address.city")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testQuotedMultiPartAsLiteral() {
    // Quoted strings are literals - "user.name" searches for the term user.name
    // Dots don't require special escaping in query_string
    assertEqual(
        "search \"user.name\" source=logs",
        filter(
            relation("logs"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("user.name")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  // ========================================================================
  // Tests for mixed search text and field references
  // ========================================================================

  @Test
  public void testMixedSearchTextAndFunctions() {
    // Mix of search text and functions - only bare text should be wrapped
    assertEqual(
        "search error AND status > 400 source=logs",
        filter(
            relation("logs"),
            and(
                function(
                    "query_string",
                    unresolvedArg("query", stringLiteral("error")),
                    unresolvedArg("default_operator", stringLiteral("AND"))),
                compare(">", field("status"), intLiteral(400)))));
  }

  @Test
  public void testMultiPartWithSinglePart() {
    // Mix of single and multi-part identifiers
    assertEqual(
        "search error AND response.time source=logs",
        filter(
            relation("logs"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("(error AND response.time)")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testSearchContextOnlyInSearchCommand() {
    // Only the search command should have search context enabled
    // "error" here should be treated as free text search
    assertEqual(
        "search error source=logs",
        filter(
            relation("logs"),
            function(
                "query_string",
                unresolvedArg("query", stringLiteral("error")),
                unresolvedArg("default_operator", stringLiteral("AND")))));
  }

  @Test
  public void testMixedSearchAndWhereContexts() {
    // Search has search context, WHERE does not
    assertEqual(
        "search error source=logs | where status > 400",
        filter(
            filter(
                relation("logs"),
                function(
                    "query_string",
                    unresolvedArg("query", stringLiteral("error")),
                    unresolvedArg("default_operator", stringLiteral("AND")))),
            compare(">", field("status"), intLiteral(400))));
  }

  @Test
  public void testComplexNestedFunctionsWithSearchText() {
    // Complex case with search text and nested functions
    assertEqual(
        "search error source=logs | where case(level = 'ERROR', 1, level = 'WARN', 2, level ="
            + " 'INFO', 0) > 0",
        filter(
            filter(
                relation("logs"),
                function(
                    "query_string",
                    unresolvedArg("query", stringLiteral("error")),
                    unresolvedArg("default_operator", stringLiteral("AND")))),
            compare(
                ">",
                new Case(
                    null,
                    Arrays.asList(
                        new When(
                            compare("=", field("level"), stringLiteral("ERROR")), intLiteral(1)),
                        new When(
                            compare("=", field("level"), stringLiteral("WARN")), intLiteral(2)),
                        new When(
                            compare("=", field("level"), stringLiteral("INFO")), intLiteral(0))),
                    Optional.empty()),
                intLiteral(0))));
  }

  @Test
  public void testMultipleLogicalExpressionsInSearch() {
    // Multiple logical expressions in search command
    assertEqual(
        "search error warning source=logs status >= 400",
        filter(
            relation("logs"),
            and(
                function(
                    "query_string",
                    unresolvedArg("query", stringLiteral("(error AND warning)")),
                    unresolvedArg("default_operator", stringLiteral("AND"))),
                compare(">=", field("status"), intLiteral(400)))));
  }

  @Test
  public void testParenthesizedExpressionsPreserveContext() {
    // Parenthesized expressions should preserve their context
    assertEqual(
        "source=t (field_a > 5 AND field_b < 10)",
        filter(
            relation("t"),
            and(
                compare(">", field("field_a"), intLiteral(5)),
                compare("<", field("field_b"), intLiteral(10)))));
  }

  @Test
  public void testFunctionInWhereClause() {
    // Functions in WHERE clause (not search context) should work normally
    assertEqual(
        "source=t | where like(field_a, 'pattern')",
        filter(relation("t"), function("like", field("field_a"), stringLiteral("pattern"))));
  }

  @Test
  public void testEvalWithFunctionsNotInSearchContext() {
    // EVAL expressions should not be in search context - field references should not be wrapped
    assertEqual(
        "source=t | eval new_field = old_field + 10",
        eval(
            relation("t"),
            let(field("new_field"), function("+", field("old_field"), intLiteral(10)))));
  }
}
