/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;


public class OpenSearchFunctionsTest extends ExpressionTestBase {
  private final NamedArgumentExpression field = new NamedArgumentExpression(
      "field", DSL.literal("message"));
  private final NamedArgumentExpression fields = new NamedArgumentExpression(
      "fields", DSL.literal(new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
          "title", ExprValueUtils.floatValue(1.F),
          "body", ExprValueUtils.floatValue(.3F))))));
  private final NamedArgumentExpression query = new NamedArgumentExpression(
      "query", DSL.literal("search query"));
  private final NamedArgumentExpression analyzer = new NamedArgumentExpression(
      "analyzer", DSL.literal("keyword"));
  private final NamedArgumentExpression autoGenerateSynonymsPhrase = new NamedArgumentExpression(
      "auto_generate_synonyms_phrase", DSL.literal("true"));
  private final NamedArgumentExpression fuzziness = new NamedArgumentExpression(
      "fuzziness", DSL.literal("AUTO"));
  private final NamedArgumentExpression maxExpansions = new NamedArgumentExpression(
      "max_expansions", DSL.literal("10"));
  private final NamedArgumentExpression prefixLength = new NamedArgumentExpression(
      "prefix_length", DSL.literal("1"));
  private final NamedArgumentExpression fuzzyTranspositions = new NamedArgumentExpression(
      "fuzzy_transpositions", DSL.literal("false"));
  private final NamedArgumentExpression fuzzyRewrite = new NamedArgumentExpression(
      "fuzzy_rewrite", DSL.literal("rewrite method"));
  private final NamedArgumentExpression lenient = new NamedArgumentExpression(
      "lenient", DSL.literal("true"));
  private final NamedArgumentExpression operator = new NamedArgumentExpression(
      "operator", DSL.literal("OR"));
  private final NamedArgumentExpression minimumShouldMatch = new NamedArgumentExpression(
      "minimum_should_match", DSL.literal("1"));
  private final NamedArgumentExpression zeroTermsQueryAll = new NamedArgumentExpression(
      "zero_terms_query", DSL.literal("ALL"));
  private final NamedArgumentExpression zeroTermsQueryNone = new NamedArgumentExpression(
      "zero_terms_query", DSL.literal("None"));
  private final NamedArgumentExpression boost = new NamedArgumentExpression(
      "boost", DSL.literal("2.0"));
  private final NamedArgumentExpression slop = new NamedArgumentExpression(
      "slop", DSL.literal("3"));

  @Test
  void match() {
    FunctionExpression expr = dsl.match(field, query);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(field, query, analyzer);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(field, query, analyzer, autoGenerateSynonymsPhrase);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite, lenient);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite, lenient, operator);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite, lenient, operator);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite, lenient, operator, minimumShouldMatch);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite, lenient, operator, minimumShouldMatch,
        zeroTermsQueryAll);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite, lenient, operator, minimumShouldMatch,
        zeroTermsQueryNone, boost);
    assertEquals(BOOLEAN, expr.type());
  }

  @Test
  void match_phrase() {
    for (FunctionExpression expr : match_phrase_dsl_expressions()) {
      assertEquals(BOOLEAN, expr.type());
    }
  }


  List<FunctionExpression> match_phrase_dsl_expressions() {
    return List.of(
      dsl.match_phrase(field, query),
      dsl.match_phrase(field, query, analyzer),
      dsl.match_phrase(field, query, analyzer, zeroTermsQueryAll),
      dsl.match_phrase(field, query, analyzer, zeroTermsQueryNone, slop)
    );
  }

  @Test
  void match_in_memory() {
    FunctionExpression expr = dsl.match(field, query);
    assertThrows(UnsupportedOperationException.class,
        () -> expr.valueOf(valueEnv()),
        "OpenSearch defined function [match] is only supported in WHERE and HAVING clause.");
  }

  @Test
  void match_to_string() {
    FunctionExpression expr = dsl.match(field, query);
    assertEquals("match(field=\"message\", query=\"search query\")", expr.toString());
  }

  @Test
  void multi_match() {
    FunctionExpression expr = dsl.multi_match(fields, query);
    assertEquals(String.format("multi_match(fields=%s, query=%s)",
            fields.getValue(), query.getValue()),
        expr.toString());
  }

  @Test
  void simple_query_string() {
    FunctionExpression expr = dsl.simple_query_string(fields, query);
    assertEquals(String.format("simple_query_string(fields=%s, query=%s)",
            fields.getValue(), query.getValue()),
        expr.toString());
  }

  @Test
  void query_string() {
    FunctionExpression expr = dsl.query_string(fields, query);
    assertEquals(String.format("query_string(fields=%s, query=%s)",
            fields.getValue(), query.getValue()),
        expr.toString());
  }
}
