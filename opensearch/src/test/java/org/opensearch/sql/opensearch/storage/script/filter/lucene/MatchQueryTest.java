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
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MatchQueryTest {
  private final MatchQuery matchQuery = new MatchQuery();
  private final FunctionName matchName = FunctionName.of("match");
  private final FunctionName matchQueryName = FunctionName.of("matchquery");
  private final FunctionName matchQueryWithUnderscoreName = FunctionName.of("match_query");
  private final FunctionName[] functionNames = {
    matchName, matchQueryName, matchQueryWithUnderscoreName
  };

  static Stream<List<Expression>> generateValidData() {
    return Stream.of(
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("analyzer", DSL.literal("standard"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("auto_generate_synonyms_phrase_query", DSL.literal("true"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("fuzziness", DSL.literal("AUTO"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("max_expansions", DSL.literal("50"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("prefix_length", DSL.literal("0"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("fuzzy_transpositions", DSL.literal("true"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("fuzzy_rewrite", DSL.literal("constant_score"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("lenient", DSL.literal("false"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("operator", DSL.literal("OR"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("minimum_should_match", DSL.literal("3"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("zero_terms_query", DSL.literal("NONE"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("zero_terms_query", DSL.literal("none"))),
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", DSL.literal("query_value")),
            DSL.namedArgument("boost", DSL.literal("1"))));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters(List<Expression> validArgs) {
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(validArgs)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class, () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("field", "field_value"));
    assertThrows(
        SyntaxCheckException.class, () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            namedArgument("query", "query_value"),
            namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(
        SemanticCheckException.class, () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters_matchquery_syntax(List<Expression> validArgs) {
    Assertions.assertNotNull(
        matchQuery.build(new MatchExpression(validArgs, MatchQueryTest.this.matchQueryName)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments_matchquery_syntax() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments, MatchQueryTest.this.matchQueryName)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument_matchquery_syntax() {
    List<Expression> arguments = List.of(namedArgument("field", "field_value"));
    assertThrows(
        SyntaxCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments, MatchQueryTest.this.matchQueryName)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter_matchquery_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            namedArgument("query", "query_value"),
            namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(
        SemanticCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments, MatchQueryTest.this.matchQueryName)));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters_match_query_syntax(List<Expression> validArgs) {
    Assertions.assertNotNull(
        matchQuery.build(
            new MatchExpression(validArgs, MatchQueryTest.this.matchQueryWithUnderscoreName)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments_match_query_syntax() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () ->
            matchQuery.build(
                new MatchExpression(arguments, MatchQueryTest.this.matchQueryWithUnderscoreName)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument_match_query_syntax() {
    List<Expression> arguments = List.of(namedArgument("field", "field_value"));
    assertThrows(
        SyntaxCheckException.class,
        () ->
            matchQuery.build(
                new MatchExpression(arguments, MatchQueryTest.this.matchQueryWithUnderscoreName)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter_match_query_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            namedArgument("query", "query_value"),
            namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(
        SemanticCheckException.class,
        () ->
            matchQuery.build(
                new MatchExpression(arguments, MatchQueryTest.this.matchQueryWithUnderscoreName)));
  }

  private NamedArgumentExpression namedArgument(String name, String value) {
    return DSL.namedArgument(name, DSL.literal(value));
  }

  private class MatchExpression extends FunctionExpression {

    public MatchExpression(List<Expression> arguments) {
      super(MatchQueryTest.this.matchName, arguments);
    }

    public MatchExpression(List<Expression> arguments, FunctionName funcName) {
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
