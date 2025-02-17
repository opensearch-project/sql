/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedArgumentExpression;

public class FunctionUtilsTest {
  @Test
  void return_empty_optional_for_empty_arguments_list() {
    // Given
    List<Expression> emptyList = Collections.emptyList();

    // When
    Optional<ExprValue> result = FunctionUtils.getNamedArgumentValue(emptyList, "anyArg");

    // Then
    assertTrue(result.isEmpty());
  }

  @Test
  void return_empty_optional_when_no_matching_argument_found() {
    // Given
    NamedArgumentExpression namedArg = Mockito.mock(NamedArgumentExpression.class);
    when(namedArg.getArgName()).thenReturn("differentArg");
    List<Expression> arguments = Collections.singletonList(namedArg);

    // When
    Optional<ExprValue> result = FunctionUtils.getNamedArgumentValue(arguments, "searchedArg");

    // Then
    assertTrue(result.isEmpty());
  }

  @Test
  void return_value_when_matching_argument_found() {
    // Given
    ExprValue mockValue = Mockito.mock(ExprValue.class);
    Expression mockExpr = Mockito.mock(Expression.class);
    when(mockExpr.valueOf()).thenReturn(mockValue);

    NamedArgumentExpression namedArg = Mockito.mock(NamedArgumentExpression.class);
    when(namedArg.getArgName()).thenReturn("targetArg");
    when(namedArg.getValue()).thenReturn(mockExpr);

    List<Expression> arguments = Collections.singletonList(namedArg);

    // When
    Optional<ExprValue> result = FunctionUtils.getNamedArgumentValue(arguments, "targetArg");

    // Then
    assertTrue(result.isPresent());
    assertEquals(mockValue, result.get());
  }

  @Test
  void case_insensitive_when_matching_argument_names() {
    // Given
    ExprValue mockValue = Mockito.mock(ExprValue.class);
    Expression mockExpr = Mockito.mock(Expression.class);
    when(mockExpr.valueOf()).thenReturn(mockValue);

    NamedArgumentExpression namedArg = Mockito.mock(NamedArgumentExpression.class);
    when(namedArg.getArgName()).thenReturn("TARGETARG");
    when(namedArg.getValue()).thenReturn(mockExpr);

    List<Expression> arguments = Collections.singletonList(namedArg);

    // When
    Optional<ExprValue> result = FunctionUtils.getNamedArgumentValue(arguments, "targetArg");

    // Then
    assertTrue(result.isPresent());
    assertEquals(mockValue, result.get());
  }

  @Test
  void ignore_non_named_argument_expressions() {
    // Given
    Expression regularExpression = Mockito.mock(Expression.class);
    NamedArgumentExpression namedArg = Mockito.mock(NamedArgumentExpression.class);
    when(namedArg.getArgName()).thenReturn("targetArg");

    ExprValue mockValue = Mockito.mock(ExprValue.class);
    Expression mockExpr = Mockito.mock(Expression.class);
    when(mockExpr.valueOf()).thenReturn(mockValue);
    when(namedArg.getValue()).thenReturn(mockExpr);

    List<Expression> arguments = Arrays.asList(regularExpression, namedArg);

    // When
    Optional<ExprValue> result = FunctionUtils.getNamedArgumentValue(arguments, "targetArg");

    // Then
    assertTrue(result.isPresent());
    assertEquals(mockValue, result.get());
  }
}
