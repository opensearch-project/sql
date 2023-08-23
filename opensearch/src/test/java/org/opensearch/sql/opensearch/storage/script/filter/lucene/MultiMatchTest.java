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
import org.opensearch.sql.common.antlr.SyntaxCheckException;
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
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.expression.OpenSearchDSL;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MultiMatchQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class MultiMatchTest {
  private final MultiMatchQuery multiMatchQuery = new MultiMatchQuery();
  private final FunctionName multiMatchName = FunctionName.of("multimatch");
  private final FunctionName snakeCaseMultiMatchName = FunctionName.of("multi_match");
  private final FunctionName multiMatchQueryName = FunctionName.of("multimatchquery");
  private static final LiteralExpression fields_value =
      DSL.literal(
          new ExprTupleValue(
              new LinkedHashMap<>(
                  ImmutableMap.of(
                      "title", ExprValueUtils.floatValue(1.F),
                      "body", ExprValueUtils.floatValue(.3F)))));
  private static final LiteralExpression query_value = DSL.literal("query_value");

  static Stream<List<Expression>> generateValidData() {
    return Stream.of(
        List.of(DSL.namedArgument("fields", fields_value), DSL.namedArgument("query", query_value)),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("analyzer", DSL.literal("simple"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("auto_generate_synonyms_phrase_query", DSL.literal("true"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("boost", DSL.literal("1.3"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("cutoff_frequency", DSL.literal("4.2"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("fuzziness", DSL.literal("AUTO:2,4"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("fuzzy_transpositions", DSL.literal("true"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("lenient", DSL.literal("true"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("max_expansions", DSL.literal("7"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("minimum_should_match", DSL.literal("4"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("operator", DSL.literal("AND"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("prefix_length", DSL.literal("7"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("tie_breaker", DSL.literal("0.3"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("type", DSL.literal("cross_fields"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("zero_terms_query", DSL.literal("ALL"))),
        List.of(
            DSL.namedArgument("fields", fields_value),
            DSL.namedArgument("query", query_value),
            DSL.namedArgument("zero_terms_query", DSL.literal("all"))));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters_multiMatch(List<Expression> validArgs) {
    Assertions.assertNotNull(multiMatchQuery.build(new MultiMatchExpression(validArgs)));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters_multi_match(List<Expression> validArgs) {
    Assertions.assertNotNull(
        multiMatchQuery.build(new MultiMatchExpression(validArgs, snakeCaseMultiMatchName)));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters_multiMatchQuery(List<Expression> validArgs) {
    Assertions.assertNotNull(
        multiMatchQuery.build(new MultiMatchExpression(validArgs, multiMatchQueryName)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments_multiMatch() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments_multi_match() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments, multiMatchName)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments_multiMatchQuery() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments, multiMatchQueryName)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument_multiMatch() {
    List<Expression> arguments = List.of(namedArgument("fields", fields_value));
    assertThrows(
        SyntaxCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument_multi_match() {
    List<Expression> arguments = List.of(namedArgument("fields", fields_value));
    assertThrows(
        SyntaxCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments, snakeCaseMultiMatchName)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument_multiMatchQuery() {
    List<Expression> arguments = List.of(namedArgument("fields", fields_value));
    assertThrows(
        SyntaxCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments, multiMatchQueryName)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter_multiMatch() {
    List<Expression> arguments = List.of(
        namedArgument("fields", fields_value),
        namedArgument("query", query_value),
        OpenSearchDSL.namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter_multi_match() {
    List<Expression> arguments = List.of(
        namedArgument("fields", fields_value),
        namedArgument("query", query_value),
        OpenSearchDSL.namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments, snakeCaseMultiMatchName)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter_multiMatchQuery() {
    List<Expression> arguments = List.of(
        namedArgument("fields", fields_value),
        namedArgument("query", query_value),
        OpenSearchDSL.namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> multiMatchQuery.build(new MultiMatchExpression(arguments, multiMatchQueryName)));
  }

  private NamedArgumentExpression namedArgument(String name, LiteralExpression value) {
    return DSL.namedArgument(name, value);
  }

  private class MultiMatchExpression extends FunctionExpression {
    public MultiMatchExpression(List<Expression> arguments) {
      super(multiMatchName, arguments);
    }

    public MultiMatchExpression(List<Expression> arguments, FunctionName funcName) {
      super(funcName, arguments);
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(
          "Invalid function call, "
              + "valueOf function need implementation only to support Expression interface");
    }

    @Override
    public ExprType type() {
      throw new UnsupportedOperationException(
          "Invalid function call, "
              + "type function need implementation only to support Expression interface");
    }
  }
}
