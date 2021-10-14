/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;

public class OpenSearchFunctionsTest extends ExpressionTestBase {
  private final NamedArgumentExpression field = new NamedArgumentExpression(
      "field", DSL.literal("message"));
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
  private final NamedArgumentExpression zeroTermsQuery = new NamedArgumentExpression(
      "zero_terms_query", DSL.literal("ALL"));
  private final NamedArgumentExpression boost = new NamedArgumentExpression(
      "boost", DSL.literal("2.0"));

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
        fuzzyTranspositions, fuzzyRewrite, lenient, operator, minimumShouldMatch, zeroTermsQuery);
    assertEquals(BOOLEAN, expr.type());

    expr = dsl.match(
        field, query, analyzer, autoGenerateSynonymsPhrase, fuzziness, maxExpansions, prefixLength,
        fuzzyTranspositions, fuzzyRewrite, lenient, operator, minimumShouldMatch, zeroTermsQuery,
        boost);
    assertEquals(BOOLEAN, expr.type());
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
}
