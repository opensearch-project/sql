/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
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
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.SimpleQueryStringQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SimpleQueryStringTest {
  private static final DSL dsl = new ExpressionConfig()
      .dsl(new ExpressionConfig().functionRepository());
  private final SimpleQueryStringQuery simpleQueryStringQuery = new SimpleQueryStringQuery();
  private final FunctionName simpleQueryString = FunctionName.of("simple_query_string");
  private static final LiteralExpression fields_value = DSL.literal(
      new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
          "title", ExprValueUtils.floatValue(1.F),
          "body", ExprValueUtils.floatValue(.3F)))));
  private static final LiteralExpression query_value = DSL.literal("query_value");

  static Stream<List<Expression>> generateValidData() {
    return Stream.of(
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value)
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("analyze_wildcard", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("analyzer", DSL.literal("standard"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("auto_generate_synonyms_phrase_query", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("flags", DSL.literal("PREFIX"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("flags", DSL.literal("PREFIX|NOT|AND"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("flags", DSL.literal("NOT|AND"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("flags", DSL.literal("PREFIX|not|AND"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("flags", DSL.literal("not|and"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("fuzzy_max_expansions", DSL.literal("42"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("fuzzy_prefix_length", DSL.literal("42"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("fuzzy_transpositions", DSL.literal("42"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("lenient", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("default_operator", DSL.literal("AND"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("default_operator", DSL.literal("and"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("minimum_should_match", DSL.literal("4"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("quote_field_suffix", DSL.literal(".exact"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("boost", DSL.literal("1"))
        ),
        List.of(
            dsl.namedArgument("FIELDS", fields_value),
            dsl.namedArgument("QUERY", query_value)
        ),
        List.of(
            dsl.namedArgument("FIELDS", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("ANALYZE_wildcard", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("fields", fields_value),
            dsl.namedArgument("query", query_value),
            dsl.namedArgument("analyZER", DSL.literal("standard"))
        )
      );
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters(List<Expression> validArgs) {
    Assertions.assertNotNull(simpleQueryStringQuery.build(
        new SimpleQueryStringExpression(validArgs)));
  }

  @Test
  public void test_SemanticCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(SemanticCheckException.class,
        () -> simpleQueryStringQuery.build(new SimpleQueryStringExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("fields", fields_value));
    assertThrows(SemanticCheckException.class,
        () -> simpleQueryStringQuery.build(new SimpleQueryStringExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("fields", fields_value),
        namedArgument("query", query_value),
        namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> simpleQueryStringQuery.build(new SimpleQueryStringExpression(arguments)));
  }

  private NamedArgumentExpression namedArgument(String name, String value) {
    return dsl.namedArgument(name, DSL.literal(value));
  }

  private NamedArgumentExpression namedArgument(String name, LiteralExpression value) {
    return dsl.namedArgument(name, value);
  }

  private class SimpleQueryStringExpression extends FunctionExpression {
    public SimpleQueryStringExpression(List<Expression> arguments) {
      super(SimpleQueryStringTest.this.simpleQueryString, arguments);
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
}
