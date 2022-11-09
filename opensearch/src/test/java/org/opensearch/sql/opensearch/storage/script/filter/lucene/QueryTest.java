/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.QueryQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class QueryTest {
  private static final DSL dsl = new ExpressionConfig()
      .dsl(new ExpressionConfig().functionRepository());
  private final QueryQuery queryQuery = new QueryQuery();
  private final FunctionName queryFunc = FunctionName.of("query");
  private static final LiteralExpression query_value = DSL.literal("title:query_value");

  static Stream<List<Expression>> generateValidData() {
    Expression query = dsl.namedArgument("query", query_value);
    return List.of(
        dsl.namedArgument("analyzer", DSL.literal("standard")),
        dsl.namedArgument("analyze_wildcard", DSL.literal("true")),
        dsl.namedArgument("allow_leading_wildcard", DSL.literal("true")),
        dsl.namedArgument("auto_generate_synonyms_phrase_query", DSL.literal("true")),
        dsl.namedArgument("boost", DSL.literal("1")),
        dsl.namedArgument("default_operator", DSL.literal("AND")),
        dsl.namedArgument("default_operator", DSL.literal("and")),
        dsl.namedArgument("enable_position_increments", DSL.literal("true")),
        dsl.namedArgument("escape", DSL.literal("false")),
        dsl.namedArgument("fuzziness", DSL.literal("1")),
        dsl.namedArgument("fuzzy_rewrite", DSL.literal("constant_score")),
        dsl.namedArgument("fuzzy_max_expansions", DSL.literal("42")),
        dsl.namedArgument("fuzzy_prefix_length", DSL.literal("42")),
        dsl.namedArgument("fuzzy_transpositions", DSL.literal("true")),
        dsl.namedArgument("lenient", DSL.literal("true")),
        dsl.namedArgument("max_determinized_states", DSL.literal("10000")),
        dsl.namedArgument("minimum_should_match", DSL.literal("4")),
        dsl.namedArgument("quote_analyzer", DSL.literal("standard")),
        dsl.namedArgument("phrase_slop", DSL.literal("0")),
        dsl.namedArgument("quote_field_suffix", DSL.literal(".exact")),
        dsl.namedArgument("rewrite", DSL.literal("constant_score")),
        dsl.namedArgument("type", DSL.literal("best_fields")),
        dsl.namedArgument("tie_breaker", DSL.literal("0.3")),
        dsl.namedArgument("time_zone", DSL.literal("Canada/Pacific")),
        dsl.namedArgument("ANALYZER", DSL.literal("standard")),
        dsl.namedArgument("ANALYZE_wildcard", DSL.literal("true")),
        dsl.namedArgument("Allow_Leading_wildcard", DSL.literal("true")),
        dsl.namedArgument("Auto_Generate_Synonyms_Phrase_Query", DSL.literal("true")),
        dsl.namedArgument("Boost", DSL.literal("1"))
    ).stream().map(arg -> List.of(query, arg));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters(List<Expression> validArgs) {
    Assertions.assertNotNull(queryQuery.build(
        new QueryExpression(validArgs)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(SyntaxCheckException.class,
        () -> queryQuery.build(new QueryExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_field_argument() {
    List<Expression> arguments = List.of(
        namedArgument("fields", "invalid argument"),
        namedArgument("query", query_value));
    assertThrows(SemanticCheckException.class,
        () -> queryQuery.build(new QueryExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("query", query_value),
        namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> queryQuery.build(new QueryExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_sending_parameter_multiple_times() {
    List<Expression> arguments = List.of(
            namedArgument("query", query_value),
            namedArgument("allow_leading_wildcard", DSL.literal("true")),
            namedArgument("allow_leading_wildcard", DSL.literal("true")));
    Assertions.assertThrows(SemanticCheckException.class,
            () -> queryQuery.build(new QueryExpression(arguments)));
  }

  private NamedArgumentExpression namedArgument(String name, String value) {
    return dsl.namedArgument(name, DSL.literal(value));
  }

  private NamedArgumentExpression namedArgument(String name, LiteralExpression value) {
    return dsl.namedArgument(name, value);
  }

  private class QueryExpression extends FunctionExpression {
    public QueryExpression(List<Expression> arguments) {
      super(QueryTest.this.queryFunc, arguments);
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException("Invalid function call, "
          + "valueOf function need implementation only to support Expression interface");
    }

    @Override
    public ExprType type() {
      throw new UnsupportedOperationException("Invalid function call, "
          + "type function need implementation only to support Expression interface");
    }
  }

  @Test
  public void test_can_get_query_name() {
    List<Expression> arguments = List.of(namedArgument("query", query_value));
    queryQuery.build(new QueryExpression(arguments));
    Assertions.assertEquals("query",
            queryQuery.getQueryName());
  }
}
