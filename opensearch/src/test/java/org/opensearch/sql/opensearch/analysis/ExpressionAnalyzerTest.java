/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.floatLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;


class ExpressionAnalyzerTest extends AnalyzerTestBase {

  @Test
  void match_bool_prefix_expression() {
    assertAnalyzeEqual(
        DSL.match_bool_prefix(
            DSL.namedArgument("field", DSL.literal("field_value1")),
            DSL.namedArgument("query", DSL.literal("sample query"))),
        AstDSL.function(
            "match_bool_prefix",
            AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
            AstDSL.unresolvedArg("query", stringLiteral("sample query"))));
  }

  @Test
  void match_bool_prefix_wrong_expression() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            analyze(
                AstDSL.function(
                    "match_bool_prefix",
                    AstDSL.unresolvedArg("field", stringLiteral("fieldA")),
                    AstDSL.unresolvedArg("query", floatLiteral(1.2f)))));
  }

  @Test
  void multi_match_expression() {
    assertAnalyzeEqual(
        DSL.multi_match(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of("field_value1", ExprValueUtils.floatValue(1.F)))))),
            DSL.namedArgument("query", DSL.literal("sample query"))),
        AstDSL.function(
            "multi_match",
            AstDSL.unresolvedArg("fields", new RelevanceFieldList(Map.of("field_value1", 1.F))),
            AstDSL.unresolvedArg("query", stringLiteral("sample query"))));
  }

  @Test
  void multi_match_expression_with_params() {
    assertAnalyzeEqual(
        DSL.multi_match(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of("field_value1", ExprValueUtils.floatValue(1.F)))))),
            DSL.namedArgument("query", DSL.literal("sample query")),
            DSL.namedArgument("analyzer", DSL.literal("keyword"))),
        AstDSL.function(
            "multi_match",
            AstDSL.unresolvedArg("fields", new RelevanceFieldList(Map.of("field_value1", 1.F))),
            AstDSL.unresolvedArg("query", stringLiteral("sample query")),
            AstDSL.unresolvedArg("analyzer", stringLiteral("keyword"))));
  }

  @Test
  void multi_match_expression_two_fields() {
    assertAnalyzeEqual(
        DSL.multi_match(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of(
                                "field_value1", ExprValueUtils.floatValue(1.F),
                                "field_value2", ExprValueUtils.floatValue(.3F)))))),
            DSL.namedArgument("query", DSL.literal("sample query"))),
        AstDSL.function(
            "multi_match",
            AstDSL.unresolvedArg(
                "fields",
                new RelevanceFieldList(ImmutableMap.of("field_value1", 1.F, "field_value2", .3F))),
            AstDSL.unresolvedArg("query", stringLiteral("sample query"))));
  }

  @Test
  void simple_query_string_expression() {
    assertAnalyzeEqual(
        DSL.simple_query_string(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of("field_value1", ExprValueUtils.floatValue(1.F)))))),
            DSL.namedArgument("query", DSL.literal("sample query"))),
        AstDSL.function(
            "simple_query_string",
            AstDSL.unresolvedArg("fields", new RelevanceFieldList(Map.of("field_value1", 1.F))),
            AstDSL.unresolvedArg("query", stringLiteral("sample query"))));
  }

  @Test
  void simple_query_string_expression_with_params() {
    assertAnalyzeEqual(
        DSL.simple_query_string(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of("field_value1", ExprValueUtils.floatValue(1.F)))))),
            DSL.namedArgument("query", DSL.literal("sample query")),
            DSL.namedArgument("analyzer", DSL.literal("keyword"))),
        AstDSL.function(
            "simple_query_string",
            AstDSL.unresolvedArg("fields", new RelevanceFieldList(Map.of("field_value1", 1.F))),
            AstDSL.unresolvedArg("query", stringLiteral("sample query")),
            AstDSL.unresolvedArg("analyzer", stringLiteral("keyword"))));
  }

  @Test
  void simple_query_string_expression_two_fields() {
    assertAnalyzeEqual(
        DSL.simple_query_string(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of(
                                "field_value1", ExprValueUtils.floatValue(1.F),
                                "field_value2", ExprValueUtils.floatValue(.3F)))))),
            DSL.namedArgument("query", DSL.literal("sample query"))),
        AstDSL.function(
            "simple_query_string",
            AstDSL.unresolvedArg(
                "fields",
                new RelevanceFieldList(ImmutableMap.of("field_value1", 1.F, "field_value2", .3F))),
            AstDSL.unresolvedArg("query", stringLiteral("sample query"))));
  }

  @Test
  void query_expression() {
    assertAnalyzeEqual(
        DSL.query(DSL.namedArgument("query", DSL.literal("field:query"))),
        AstDSL.function("query", AstDSL.unresolvedArg("query", stringLiteral("field:query"))));
  }

  @Test
  void query_string_expression() {
    assertAnalyzeEqual(
        DSL.query_string(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of("field_value1", ExprValueUtils.floatValue(1.F)))))),
            DSL.namedArgument("query", DSL.literal("query_value"))),
        AstDSL.function(
            "query_string",
            AstDSL.unresolvedArg("fields", new RelevanceFieldList(Map.of("field_value1", 1.F))),
            AstDSL.unresolvedArg("query", stringLiteral("query_value"))));
  }

  @Test
  void query_string_expression_with_params() {
    assertAnalyzeEqual(
        DSL.query_string(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of("field_value1", ExprValueUtils.floatValue(1.F)))))),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("escape", DSL.literal("false"))),
        AstDSL.function(
            "query_string",
            AstDSL.unresolvedArg("fields", new RelevanceFieldList(Map.of("field_value1", 1.F))),
            AstDSL.unresolvedArg("query", stringLiteral("query_value")),
            AstDSL.unresolvedArg("escape", stringLiteral("false"))));
  }

  @Test
  void query_string_expression_two_fields() {
    assertAnalyzeEqual(
        DSL.query_string(
            DSL.namedArgument(
                "fields",
                DSL.literal(
                    new ExprTupleValue(
                        new LinkedHashMap<>(
                            ImmutableMap.of(
                                "field_value1", ExprValueUtils.floatValue(1.F),
                                "field_value2", ExprValueUtils.floatValue(.3F)))))),
            DSL.namedArgument("query", DSL.literal("query_value"))),
        AstDSL.function(
            "query_string",
            AstDSL.unresolvedArg(
                "fields",
                new RelevanceFieldList(ImmutableMap.of("field_value1", 1.F, "field_value2", .3F))),
            AstDSL.unresolvedArg("query", stringLiteral("query_value"))));
  }

  @Test
  void wildcard_query_expression() {
    assertAnalyzeEqual(
        DSL.wildcard_query(
            DSL.namedArgument("field", DSL.literal("test")),
            DSL.namedArgument("query", DSL.literal("query_value*"))),
        AstDSL.function(
            "wildcard_query",
            unresolvedArg("field", stringLiteral("test")),
            unresolvedArg("query", stringLiteral("query_value*"))));
  }

  @Test
  void wildcard_query_expression_all_params() {
    assertAnalyzeEqual(
        DSL.wildcard_query(
            DSL.namedArgument("field", DSL.literal("test")),
            DSL.namedArgument("query", DSL.literal("query_value*")),
            DSL.namedArgument("boost", DSL.literal("1.5")),
            DSL.namedArgument("case_insensitive", DSL.literal("true")),
            DSL.namedArgument("rewrite", DSL.literal("scoring_boolean"))),
        AstDSL.function(
            "wildcard_query",
            unresolvedArg("field", stringLiteral("test")),
            unresolvedArg("query", stringLiteral("query_value*")),
            unresolvedArg("boost", stringLiteral("1.5")),
            unresolvedArg("case_insensitive", stringLiteral("true")),
            unresolvedArg("rewrite", stringLiteral("scoring_boolean"))));
  }

  @Test
  public void match_phrase_prefix_all_params() {
    assertAnalyzeEqual(
        DSL.match_phrase_prefix(
            DSL.namedArgument("field", "field_value1"),
            DSL.namedArgument("query", "search query"),
            DSL.namedArgument("slop", "3"),
            DSL.namedArgument("boost", "1.5"),
            DSL.namedArgument("analyzer", "standard"),
            DSL.namedArgument("max_expansions", "4"),
            DSL.namedArgument("zero_terms_query", "NONE")),
        AstDSL.function(
            "match_phrase_prefix",
            unresolvedArg("field", stringLiteral("field_value1")),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("slop", stringLiteral("3")),
            unresolvedArg("boost", stringLiteral("1.5")),
            unresolvedArg("analyzer", stringLiteral("standard")),
            unresolvedArg("max_expansions", stringLiteral("4")),
            unresolvedArg("zero_terms_query", stringLiteral("NONE"))));
  }

  @Test
  void score_function_expression() {
    assertAnalyzeEqual(
        DSL.score(
            DSL.namedArgument(
                "RelevanceQuery",
                DSL.match_phrase_prefix(
                    DSL.namedArgument("field", "field_value1"),
                    DSL.namedArgument("query", "search query"),
                    DSL.namedArgument("slop", "3")))),
        AstDSL.function(
            "score",
            unresolvedArg(
                "RelevanceQuery",
                AstDSL.function(
                    "match_phrase_prefix",
                    unresolvedArg("field", stringLiteral("field_value1")),
                    unresolvedArg("query", stringLiteral("search query")),
                    unresolvedArg("slop", stringLiteral("3"))))));
  }

  @Test
  void score_function_with_boost() {
    assertAnalyzeEqual(
        DSL.score(
            DSL.namedArgument(
                "RelevanceQuery",
                DSL.match_phrase_prefix(
                    DSL.namedArgument("field", "field_value1"),
                    DSL.namedArgument("query", "search query"),
                    DSL.namedArgument("boost", "3.0"))),
            DSL.namedArgument("boost", "2")),
        AstDSL.function(
            "score",
            unresolvedArg(
                "RelevanceQuery",
                AstDSL.function(
                    "match_phrase_prefix",
                    unresolvedArg("field", stringLiteral("field_value1")),
                    unresolvedArg("query", stringLiteral("search query")),
                    unresolvedArg("boost", stringLiteral("3.0")))),
            unresolvedArg("boost", stringLiteral("2"))));
  }

  @Test
  void score_query_function_expression() {
    assertAnalyzeEqual(
        DSL.score_query(
            DSL.namedArgument(
                "RelevanceQuery",
                DSL.wildcard_query(
                    DSL.namedArgument("field", "field_value1"),
                    DSL.namedArgument("query", "search query")))),
        AstDSL.function(
            "score_query",
            unresolvedArg(
                "RelevanceQuery",
                AstDSL.function(
                    "wildcard_query",
                    unresolvedArg("field", stringLiteral("field_value1")),
                    unresolvedArg("query", stringLiteral("search query"))))));
  }

  @Test
  void score_query_function_with_boost() {
    assertAnalyzeEqual(
        DSL.score_query(
            DSL.namedArgument(
                "RelevanceQuery",
                DSL.wildcard_query(
                    DSL.namedArgument("field", "field_value1"),
                    DSL.namedArgument("query", "search query"))),
            DSL.namedArgument("boost", "2.0")),
        AstDSL.function(
            "score_query",
            unresolvedArg(
                "RelevanceQuery",
                AstDSL.function(
                    "wildcard_query",
                    unresolvedArg("field", stringLiteral("field_value1")),
                    unresolvedArg("query", stringLiteral("search query")))),
            unresolvedArg("boost", stringLiteral("2.0"))));
  }

  @Test
  void scorequery_function_expression() {
    assertAnalyzeEqual(
        DSL.scorequery(
            DSL.namedArgument(
                "RelevanceQuery",
                DSL.simple_query_string(
                    DSL.namedArgument("field", "field_value1"),
                    DSL.namedArgument("query", "search query"),
                    DSL.namedArgument("slop", "3")))),
        AstDSL.function(
            "scorequery",
            unresolvedArg(
                "RelevanceQuery",
                AstDSL.function(
                    "simple_query_string",
                    unresolvedArg("field", stringLiteral("field_value1")),
                    unresolvedArg("query", stringLiteral("search query")),
                    unresolvedArg("slop", stringLiteral("3"))))));
  }

  @Test
  void scorequery_function_with_boost() {
    assertAnalyzeEqual(
        DSL.scorequery(
            DSL.namedArgument(
                "RelevanceQuery",
                DSL.simple_query_string(
                    DSL.namedArgument("field", "field_value1"),
                    DSL.namedArgument("query", "search query"),
                    DSL.namedArgument("slop", "3"))),
            DSL.namedArgument("boost", "2.0")),
        AstDSL.function(
            "scorequery",
            unresolvedArg(
                "RelevanceQuery",
                AstDSL.function(
                    "simple_query_string",
                    unresolvedArg("field", stringLiteral("field_value1")),
                    unresolvedArg("query", stringLiteral("search query")),
                    unresolvedArg("slop", stringLiteral("3")))),
            unresolvedArg("boost", stringLiteral("2.0"))));
  }

  protected Expression analyze(UnresolvedExpression unresolvedExpression) {
    return expressionAnalyzer.analyze(unresolvedExpression, analysisContext);
  }

  protected void assertAnalyzeEqual(
      Expression expected, UnresolvedExpression unresolvedExpression) {
    assertEquals(expected, analyze(unresolvedExpression));
  }
}
