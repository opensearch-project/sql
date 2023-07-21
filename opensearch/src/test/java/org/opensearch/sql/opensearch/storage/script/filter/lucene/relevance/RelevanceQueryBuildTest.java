/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RelevanceQueryBuildTest {

  public static final NamedArgumentExpression FIELD_ARG = namedArgument("field", "field_A");
  public static final NamedArgumentExpression QUERY_ARG = namedArgument("query", "find me");
  private RelevanceQuery query;
  private QueryBuilder queryBuilder;
  private final Map<String, RelevanceQuery.QueryBuilderStep<QueryBuilder>> queryBuildActions =
      ImmutableMap.<String, RelevanceQuery.QueryBuilderStep<QueryBuilder>>builder()
         .put("boost", (k, v) -> k.boost(Float.parseFloat(v.stringValue()))).build();

  @BeforeEach
  public void setUp() {
    query = mock(RelevanceQuery.class, withSettings().useConstructor(queryBuildActions)
        .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    queryBuilder = mock(QueryBuilder.class);
    when(query.createQueryBuilder(any())).thenReturn(queryBuilder);
    String queryName = "mock_query";
    when(queryBuilder.queryName()).thenReturn(queryName);
    when(queryBuilder.getWriteableName()).thenReturn(queryName);
    when(query.getQueryName()).thenReturn(queryName);
  }

  @Test
  void throws_SemanticCheckException_when_same_argument_twice() {
    FunctionExpression expr = createCall(List.of(FIELD_ARG, QUERY_ARG,
        namedArgument("boost", "2.3"),
        namedArgument("boost", "2.4")));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> query.build(expr));
    assertEquals("Parameter 'boost' can only be specified once.", exception.getMessage());
  }

  @Test
  void throws_SemanticCheckException_when_wrong_argument_name() {
    FunctionExpression expr =
        createCall(List.of(FIELD_ARG, QUERY_ARG, namedArgument("wrongArg", "value")));

    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> query.build(expr));
    assertEquals("Parameter wrongarg is invalid for mock_query function.",
        exception.getMessage());
  }

  @Test
  void calls_action_when_correct_argument_name() {
    FunctionExpression expr =
        createCall(List.of(FIELD_ARG, QUERY_ARG, namedArgument("boost", "2.3")));
    query.build(expr);

    verify(queryBuilder, times(1)).boost(2.3f);
  }

  @ParameterizedTest
  @MethodSource("insufficientArguments")
  public void throws_SyntaxCheckException_when_no_required_arguments(List<Expression> arguments) {
    SyntaxCheckException exception = assertThrows(SyntaxCheckException.class,
        () -> query.build(createCall(arguments)));
    assertEquals("mock_query requires at least two parameters", exception.getMessage());
  }

  public static Stream<List<Expression>> insufficientArguments() {
    return Stream.of(List.of(),
        List.of(namedArgument("field", "field_A")));
  }

  private static NamedArgumentExpression namedArgument(String field, String fieldValue) {
    return new NamedArgumentExpression(field, createLiteral(fieldValue));
  }

  @Test
  private static Expression createLiteral(String value) {
    return new LiteralExpression(new ExprStringValue(value));
  }

  private static FunctionExpression createCall(List<Expression> arguments) {
    return new FunctionExpression(new FunctionName("mock_function"), arguments) {
      @Override
      public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        throw new NotImplementedException("FunctionExpression.valueOf");
      }

      @Override
      public ExprType type() {
        throw new NotImplementedException("FunctionExpression.type");
      }
    };
  }
}
