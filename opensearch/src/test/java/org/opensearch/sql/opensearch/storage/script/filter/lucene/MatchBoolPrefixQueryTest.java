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
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchBoolPrefixQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MatchBoolPrefixQueryTest {
  private final MatchBoolPrefixQuery matchBoolPrefixQuery = new MatchBoolPrefixQuery();
  private final FunctionName matchBoolPrefix = FunctionName.of("match_bool_prefix");

  static Stream<List<Expression>> generateValidData() {
    NamedArgumentExpression field =
        DSL.namedArgument("field", new ReferenceExpression("field_value", OpenSearchTextType.of()));
    NamedArgumentExpression query = DSL.namedArgument("query", DSL.literal("query_value"));
    return List.of(
            DSL.namedArgument("fuzziness", DSL.literal("AUTO")),
            DSL.namedArgument("max_expansions", DSL.literal("50")),
            DSL.namedArgument("prefix_length", DSL.literal("0")),
            DSL.namedArgument("fuzzy_transpositions", DSL.literal("true")),
            DSL.namedArgument("fuzzy_rewrite", DSL.literal("constant_score")),
            DSL.namedArgument("minimum_should_match", DSL.literal("3")),
            DSL.namedArgument("boost", DSL.literal("1")),
            DSL.namedArgument("analyzer", DSL.literal("simple")),
            DSL.namedArgument("operator", DSL.literal("Or")),
            DSL.namedArgument("operator", DSL.literal("and")))
        .stream()
        .map(arg -> List.of(field, query, arg));
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_arguments(List<Expression> validArgs) {
    Assertions.assertNotNull(matchBoolPrefixQuery.build(new MatchExpression(validArgs)));
  }

  @Test
  public void test_valid_when_two_arguments() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", "query_value"));
    Assertions.assertNotNull(matchBoolPrefixQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () -> matchBoolPrefixQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments = List.of(DSL.namedArgument("field", "field_value"));
    assertThrows(
        SyntaxCheckException.class,
        () -> matchBoolPrefixQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_argument() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression("field_value", OpenSearchTextType.of())),
            DSL.namedArgument("query", "query_value"),
            DSL.namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(
        SemanticCheckException.class,
        () -> matchBoolPrefixQuery.build(new MatchExpression(arguments)));
  }

  private class MatchExpression extends FunctionExpression {
    public MatchExpression(List<Expression> arguments) {
      super(MatchBoolPrefixQueryTest.this.matchBoolPrefix, arguments);
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
