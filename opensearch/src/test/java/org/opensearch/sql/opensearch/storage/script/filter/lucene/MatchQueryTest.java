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
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MatchQueryTest {
  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());
  private final MatchQuery matchQuery = new MatchQuery();
  private final FunctionName match = FunctionName.of("match");

  static Stream<List<Expression>> generateValidData() {
    final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());
    return Stream.of(
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("analyzer", DSL.literal("standard"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("auto_generate_synonyms_phrase_query", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("fuzziness", DSL.literal("AUTO"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("max_expansions", DSL.literal("50"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("prefix_length", DSL.literal("0"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("fuzzy_transpositions", DSL.literal("true"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("fuzzy_rewrite", DSL.literal("constant_score"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("lenient", DSL.literal("false"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("operator", DSL.literal("OR"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("minimum_should_match", DSL.literal("3"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("zero_terms_query", DSL.literal("NONE"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("zero_terms_query", DSL.literal("none"))
        ),
        List.of(
            dsl.namedArgument("field", DSL.literal("field_value")),
            dsl.namedArgument("query", DSL.literal("query_value")),
            dsl.namedArgument("boost", DSL.literal("1"))
        )
    );
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters(List<Expression> validArgs) {
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(validArgs)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(SyntaxCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("field", "field_value"));
    assertThrows(SyntaxCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments)));
  }

  private NamedArgumentExpression namedArgument(String name, String value) {
    return dsl.namedArgument(name, DSL.literal(value));
  }

  private class MatchExpression extends FunctionExpression {
    public MatchExpression(List<Expression> arguments) {
      super(MatchQueryTest.this.match, arguments);
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
