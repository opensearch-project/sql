/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.*;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.opensearch.sql.ast.expression.RelevanceFieldList;

public class AstWhereClauseTest extends AstBuilderTest {
  @Test
  public void testBinaryOperationExprWithParentheses() {
    assertEqual(
        "source = t | where a = (1 + 2) * 3",
        filter(
            relation("t"),
            compare(
                "=",
                field("a"),
                function("*", function("+", intLiteral(1), intLiteral(2)), intLiteral(3)))));
  }

  @Test
  public void testBinaryOperationExprPrecedence() {
    assertEqual(
        "source = t | where a = 1 + 2 * 3",
        filter(
            relation("t"),
            compare(
                "=",
                field("a"),
                function("+", intLiteral(1), function("*", intLiteral(2), intLiteral(3))))));
  }

  @Test
  public void canBuildMatchRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where match('message', 'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "match",
                unresolvedArg("field", qualifiedName("message")),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void canBuildMulti_matchRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where multi_match(['field1', 'field2' ^ 3.2],"
            + "'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "multi_match",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void canBuildSimple_query_stringRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where simple_query_string(['field1', 'field2' ^ 3.2],"
            + "'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "simple_query_string",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void canBuildQuery_stringRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where query_string(['field1', 'field2' ^ 3.2],"
            + "'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "query_string",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void canBuildSingleVariableExpression() {
    assertEqual("source = test | where x", filter(relation("test"), field("x")));
  }

  @Test
  public void canBuildPureBooleanExpression() {
    assertEqual("source = test | where TRUE", filter(relation("test"), booleanLiteral(true)));
  }

  @Test
  public void canBuildVariableBooleanExpression() {
    assertEqual(
        "source = test | where x OR y", filter(relation("test"), or(field("x"), field("y"))));
  }

  @Test
  public void canBuildPlainParenthesizedExpression() {
    assertEqual(
        "source = test | where (1 >= 0)",
        filter(relation("test"), compare(">=", intLiteral(1), intLiteral(0))));
  }

  @Test
  public void canBuildMultiPartParenthesizedExpression() {
    assertEqual(
        "source = test | where (day_of_week_i < 2) OR (day_of_week_i > 5)",
        filter(
            relation("test"),
            or(
                compare("<", field("day_of_week_i"), intLiteral(2)),
                compare(">", field("day_of_week_i"), intLiteral(5)))));
  }
}
