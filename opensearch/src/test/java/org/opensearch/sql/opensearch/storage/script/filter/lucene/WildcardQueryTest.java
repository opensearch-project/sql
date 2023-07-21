/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.expression.DSL.namedArgument;

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
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.WildcardQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class WildcardQueryTest {
  private final WildcardQuery wildcardQueryQuery = new WildcardQuery();
  private static final FunctionName wildcardQueryFunc = FunctionName.of("wildcard_query");

  static Stream<List<Expression>> generateValidData() {
    return Stream.of(
        List.of(
            namedArgument("field",
                new ReferenceExpression("title", OpenSearchTextType.of())),
            namedArgument("query", "query_value*"),
            namedArgument("boost", "0.7"),
            namedArgument("case_insensitive", "false"),
            namedArgument("rewrite", "constant_score_boolean")
        )
    );
  }

  @ParameterizedTest
  @MethodSource("generateValidData")
  public void test_valid_parameters(List<Expression> validArgs) {
    Assertions.assertNotNull(wildcardQueryQuery.build(
        new WildcardQueryExpression(validArgs)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(SyntaxCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("field",
        new ReferenceExpression("title", OpenSearchTextType.of())));
    assertThrows(SyntaxCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field",
            new ReferenceExpression("title", OpenSearchTextType.of())),
        namedArgument("query", "query_value*"),
        namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> wildcardQueryQuery.build(new WildcardQueryExpression(arguments)));
  }

  private class WildcardQueryExpression extends FunctionExpression {
    public WildcardQueryExpression(List<Expression> arguments) {
      super(WildcardQueryTest.this.wildcardQueryFunc, arguments);
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
