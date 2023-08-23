/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.WideningTypeRule;
import org.opensearch.sql.exception.ExpressionEvaluationException;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class DefaultFunctionResolverTest {
  @Mock private FunctionSignature exactlyMatchFS;
  @Mock private FunctionSignature bestMatchFS;
  @Mock private FunctionSignature leastMatchFS;
  @Mock private FunctionSignature notMatchFS;
  @Mock private FunctionSignature functionSignature;
  @Mock private FunctionBuilder exactlyMatchBuilder;
  @Mock private FunctionBuilder bestMatchBuilder;
  @Mock private FunctionBuilder leastMatchBuilder;
  @Mock private FunctionBuilder notMatchBuilder;

  private FunctionName functionName = FunctionName.of("add");

  @Test
  void resolve_function_signature_exactly_match() {
    when(functionSignature.match(exactlyMatchFS)).thenReturn(WideningTypeRule.TYPE_EQUAL);
    DefaultFunctionResolver resolver =
        new DefaultFunctionResolver(
            functionName, ImmutableMap.of(exactlyMatchFS, exactlyMatchBuilder));

    assertEquals(exactlyMatchBuilder, resolver.resolve(functionSignature).getValue());
  }

  @Test
  void resolve_function_signature_best_match() {
    when(functionSignature.match(bestMatchFS)).thenReturn(1);
    when(functionSignature.match(leastMatchFS)).thenReturn(2);
    DefaultFunctionResolver resolver =
        new DefaultFunctionResolver(
            functionName,
            ImmutableMap.of(bestMatchFS, bestMatchBuilder, leastMatchFS, leastMatchBuilder));

    assertEquals(bestMatchBuilder, resolver.resolve(functionSignature).getValue());
  }

  @Test
  void resolve_function_not_match() {
    when(functionSignature.match(notMatchFS)).thenReturn(WideningTypeRule.IMPOSSIBLE_WIDENING);
    when(notMatchFS.formatTypes()).thenReturn("[INTEGER,INTEGER]");
    when(functionSignature.formatTypes()).thenReturn("[BOOLEAN,BOOLEAN]");
    DefaultFunctionResolver resolver =
        new DefaultFunctionResolver(functionName, ImmutableMap.of(notMatchFS, notMatchBuilder));

    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class, () -> resolver.resolve(functionSignature));
    assertEquals(
        "add function expected {[INTEGER,INTEGER]}, but get [BOOLEAN,BOOLEAN]",
        exception.getMessage());
  }

  @Test
  void resolve_varargs_function_signature_match() {
    functionName = FunctionName.of("concat");
    when(functionSignature.match(bestMatchFS)).thenReturn(WideningTypeRule.TYPE_EQUAL);
    when(functionSignature.getParamTypeList()).thenReturn(ImmutableList.of(STRING));
    when(bestMatchFS.getParamTypeList()).thenReturn(ImmutableList.of(ARRAY));

    DefaultFunctionResolver resolver =
        new DefaultFunctionResolver(functionName, ImmutableMap.of(bestMatchFS, bestMatchBuilder));

    assertEquals(bestMatchBuilder, resolver.resolve(functionSignature).getValue());
  }

  @Test
  void resolve_varargs_no_args_function_signature_not_match() {
    functionName = FunctionName.of("concat");
    when(functionSignature.match(bestMatchFS)).thenReturn(WideningTypeRule.TYPE_EQUAL);
    when(bestMatchFS.getParamTypeList()).thenReturn(ImmutableList.of(ARRAY));
    // Concat function with no arguments
    when(functionSignature.getParamTypeList()).thenReturn(Collections.emptyList());

    DefaultFunctionResolver resolver =
        new DefaultFunctionResolver(functionName, ImmutableMap.of(bestMatchFS, bestMatchBuilder));

    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class, () -> resolver.resolve(functionSignature));
    assertEquals("concat function expected 1-9 arguments, but got 0", exception.getMessage());
  }

  @Test
  void resolve_varargs_too_many_args_function_signature_not_match() {
    functionName = FunctionName.of("concat");
    when(functionSignature.match(bestMatchFS)).thenReturn(WideningTypeRule.TYPE_EQUAL);
    when(bestMatchFS.getParamTypeList()).thenReturn(ImmutableList.of(ARRAY));
    // Concat function with more than 9 arguments
    when(functionSignature.getParamTypeList())
        .thenReturn(
            ImmutableList.of(
                STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING));

    DefaultFunctionResolver resolver =
        new DefaultFunctionResolver(functionName, ImmutableMap.of(bestMatchFS, bestMatchBuilder));

    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class, () -> resolver.resolve(functionSignature));
    assertEquals("concat function expected 1-9 arguments, but got 10", exception.getMessage());
  }

  @Test
  void resolve_nested_function() {
    functionName = FunctionName.of("nested");
  }
}
