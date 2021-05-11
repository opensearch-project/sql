/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
class BuiltinFunctionRepositoryTest {
  @Mock
  private FunctionResolver mockfunctionResolver;
  @Mock
  private Map<FunctionName, FunctionResolver> mockMap;
  @Mock
  private FunctionName mockFunctionName;
  @Mock
  private FunctionBuilder functionExpressionBuilder;
  @Mock
  private FunctionSignature functionSignature;
  @Mock
  private Expression mockExpression;
  @Mock
  private Environment<Expression, ExprCoreType> emptyEnv;

  @Test
  void register() {
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    repo.register(mockfunctionResolver);

    verify(mockMap, times(1)).put(mockFunctionName, mockfunctionResolver);
  }

  @Test
  void compile() {
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    when(mockfunctionResolver.resolve(any())).thenReturn(functionExpressionBuilder);
    when(mockMap.containsKey(any())).thenReturn(true);
    when(mockMap.get(any())).thenReturn(mockfunctionResolver);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    repo.register(mockfunctionResolver);

    repo.compile(mockFunctionName, Arrays.asList(mockExpression));
    verify(functionExpressionBuilder, times(1)).apply(any());
  }

  @Test
  @DisplayName("resolve registered function should pass")
  void resolve() {
    when(functionSignature.getFunctionName()).thenReturn(mockFunctionName);
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    when(mockfunctionResolver.resolve(functionSignature)).thenReturn(functionExpressionBuilder);
    when(mockMap.containsKey(mockFunctionName)).thenReturn(true);
    when(mockMap.get(mockFunctionName)).thenReturn(mockfunctionResolver);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    repo.register(mockfunctionResolver);

    assertEquals(functionExpressionBuilder, repo.resolve(functionSignature));
  }

  @Test
  @DisplayName("resolve unregistered function should throw exception")
  void resolve_unregistered() {
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    when(mockMap.containsKey(any())).thenReturn(false);
    repo.register(mockfunctionResolver);

    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> repo.resolve(new FunctionSignature(FunctionName.of("unknown"), Arrays.asList())));
    assertEquals("unsupported function name: unknown", exception.getMessage());
  }
}
