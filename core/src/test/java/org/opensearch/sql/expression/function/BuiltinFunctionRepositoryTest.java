/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_BOOLEAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionRepository.DEFAULT_NAMESPACE;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
class BuiltinFunctionRepositoryTest {

  public static final String TEST_NAMESPACE = "TEST";

  @Mock
  private DefaultFunctionResolver mockfunctionResolver;
  @Mock
  private Map<String, Map<FunctionName, FunctionResolver>> mockNamespaceMap;
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

  private BuiltinFunctionRepository repo;

  @BeforeEach
  void setUp() {
    repo = new BuiltinFunctionRepository(mockNamespaceMap);
  }

  @Test
  void register() {
    when(mockNamespaceMap.get(DEFAULT_NAMESPACE)).thenReturn(mockMap);
    when(mockNamespaceMap.containsKey(DEFAULT_NAMESPACE)).thenReturn(true);
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockNamespaceMap);
    repo.register(mockfunctionResolver);

    verify(mockMap, times(1)).put(mockFunctionName, mockfunctionResolver);
  }

  @Test
  void register_under_datasource_namespace() {
    when(mockNamespaceMap.containsKey(TEST_NAMESPACE)).thenReturn(false);
    when(mockNamespaceMap.put(eq(TEST_NAMESPACE), any())).thenReturn(null);
    when(mockNamespaceMap.get(TEST_NAMESPACE)).thenReturn(mockMap);
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockNamespaceMap);
    repo.register(TEST_NAMESPACE, mockfunctionResolver);

    verify(mockNamespaceMap, times(1)).put(eq(TEST_NAMESPACE), any());
    verify(mockNamespaceMap, times(1)).get(TEST_NAMESPACE);
    verify(mockMap, times(1)).put(mockFunctionName, mockfunctionResolver);
  }

  @Test
  void compile() {
    when(mockExpression.type()).thenReturn(UNDEFINED);
    when(functionSignature.getParamTypeList()).thenReturn(Arrays.asList(UNDEFINED));
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    when(mockfunctionResolver.resolve(any())).thenReturn(
        Pair.of(functionSignature, functionExpressionBuilder));
    when(mockNamespaceMap.get(DEFAULT_NAMESPACE)).thenReturn(mockMap);
    when(mockNamespaceMap.containsKey(DEFAULT_NAMESPACE)).thenReturn(true);
    when(mockMap.containsKey(mockFunctionName)).thenReturn(true);
    when(mockMap.get(mockFunctionName)).thenReturn(mockfunctionResolver);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockNamespaceMap);
    repo.register(mockfunctionResolver);

    repo.compile(mockFunctionName, Arrays.asList(mockExpression));
    verify(functionExpressionBuilder, times(1)).apply(any());
  }


  @Test
  void compile_function_under_datasource_namespace() {
    when(mockExpression.type()).thenReturn(UNDEFINED);
    when(functionSignature.getParamTypeList()).thenReturn(Arrays.asList(UNDEFINED));
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    when(mockfunctionResolver.resolve(any())).thenReturn(
        Pair.of(functionSignature, functionExpressionBuilder));
    when(mockNamespaceMap.get(TEST_NAMESPACE)).thenReturn(mockMap);
    when(mockNamespaceMap.containsKey(TEST_NAMESPACE)).thenReturn(true);
    when(mockMap.containsKey(mockFunctionName)).thenReturn(true);
    when(mockMap.get(mockFunctionName)).thenReturn(mockfunctionResolver);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockNamespaceMap);
    repo.register(TEST_NAMESPACE, mockfunctionResolver);

    repo.compile(TEST_NAMESPACE, mockFunctionName, Arrays.asList(mockExpression));
    verify(functionExpressionBuilder, times(1)).apply(any());
  }

  @Test
  @DisplayName("resolve registered function should pass")
  void resolve() {
    when(functionSignature.getFunctionName()).thenReturn(mockFunctionName);
    when(mockfunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    when(mockfunctionResolver.resolve(functionSignature)).thenReturn(
        Pair.of(functionSignature, functionExpressionBuilder));
    when(mockNamespaceMap.get(DEFAULT_NAMESPACE)).thenReturn(mockMap);
    when(mockNamespaceMap.containsKey(DEFAULT_NAMESPACE)).thenReturn(true);
    when(mockMap.containsKey(mockFunctionName)).thenReturn(true);
    when(mockMap.get(mockFunctionName)).thenReturn(mockfunctionResolver);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockNamespaceMap);
    repo.register(mockfunctionResolver);

    assertEquals(functionExpressionBuilder,
        repo.resolve(Collections.singletonList(DEFAULT_NAMESPACE), functionSignature));
  }

  @Test
  void resolve_should_not_cast_arguments_in_cast_function() {
    when(mockExpression.toString()).thenReturn("string");
    FunctionImplementation function =
        repo.resolve(Collections.singletonList(DEFAULT_NAMESPACE),
                registerFunctionResolver(CAST_TO_BOOLEAN.getName(), DATETIME, BOOLEAN))
            .apply(ImmutableList.of(mockExpression));
    assertEquals("cast_to_boolean(string)", function.toString());
  }

  @Test
  void resolve_should_not_cast_arguments_if_same_type() {
    when(mockFunctionName.getFunctionName()).thenReturn("mock");
    when(mockExpression.toString()).thenReturn("string");
    FunctionImplementation function =
        repo.resolve(Collections.singletonList(DEFAULT_NAMESPACE),
                registerFunctionResolver(mockFunctionName, STRING, STRING))
            .apply(ImmutableList.of(mockExpression));
    assertEquals("mock(string)", function.toString());
  }

  @Test
  void resolve_should_not_cast_arguments_if_both_numbers() {
    when(mockFunctionName.getFunctionName()).thenReturn("mock");
    when(mockExpression.toString()).thenReturn("byte");
    FunctionImplementation function =
        repo.resolve(Collections.singletonList(DEFAULT_NAMESPACE),
                registerFunctionResolver(mockFunctionName, BYTE, INTEGER))
            .apply(ImmutableList.of(mockExpression));
    assertEquals("mock(byte)", function.toString());
  }

  @Test
  void resolve_should_cast_arguments() {
    when(mockFunctionName.getFunctionName()).thenReturn("mock");
    when(mockExpression.toString()).thenReturn("string");
    when(mockExpression.type()).thenReturn(STRING);

    FunctionSignature signature =
        registerFunctionResolver(mockFunctionName, STRING, BOOLEAN);
    registerFunctionResolver(CAST_TO_BOOLEAN.getName(), STRING, STRING);

    FunctionImplementation function =
        repo.resolve(Collections.singletonList(DEFAULT_NAMESPACE), signature)
            .apply(ImmutableList.of(mockExpression));
    assertEquals("mock(cast_to_boolean(string))", function.toString());
  }

  @Test
  void resolve_should_throw_exception_for_unsupported_conversion() {
    ExpressionEvaluationException error =
        assertThrows(ExpressionEvaluationException.class, () ->
            repo.resolve(Collections.singletonList(DEFAULT_NAMESPACE),
                    registerFunctionResolver(mockFunctionName, BYTE, STRUCT))
                .apply(ImmutableList.of(mockExpression)));
    assertEquals(error.getMessage(), "Type conversion to type STRUCT is not supported");
  }

  @Test
  @DisplayName("resolve unregistered function should throw exception")
  void resolve_unregistered() {
    when(mockNamespaceMap.get(DEFAULT_NAMESPACE)).thenReturn(mockMap);
    when(mockNamespaceMap.containsKey(DEFAULT_NAMESPACE)).thenReturn(true);
    when(mockMap.containsKey(any())).thenReturn(false);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockNamespaceMap);
    repo.register(mockfunctionResolver);

    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> repo.resolve(Collections.singletonList(DEFAULT_NAMESPACE),
            new FunctionSignature(FunctionName.of("unknown"), Arrays.asList())));
    assertEquals("unsupported function name: unknown", exception.getMessage());
  }

  private FunctionSignature registerFunctionResolver(FunctionName funcName,
                                                     ExprType sourceType,
                                                     ExprType targetType) {
    FunctionSignature unresolvedSignature = new FunctionSignature(
        funcName, ImmutableList.of(sourceType));
    FunctionSignature resolvedSignature = new FunctionSignature(
        funcName, ImmutableList.of(targetType));

    DefaultFunctionResolver funcResolver = mock(DefaultFunctionResolver.class);
    FunctionBuilder funcBuilder = mock(FunctionBuilder.class);

    when(mockNamespaceMap.get(DEFAULT_NAMESPACE)).thenReturn(mockMap);
    when(mockNamespaceMap.containsKey(DEFAULT_NAMESPACE)).thenReturn(true);
    when(mockMap.containsKey(eq(funcName))).thenReturn(true);
    when(mockMap.get(eq(funcName))).thenReturn(funcResolver);
    when(funcResolver.resolve(eq(unresolvedSignature))).thenReturn(
        Pair.of(resolvedSignature, funcBuilder));
    repo.register(funcResolver);

    // Relax unnecessary stubbing check because error case test doesn't call this
    lenient().doAnswer(invocation ->
        new FakeFunctionExpression(funcName, invocation.getArgument(0))
    ).when(funcBuilder).apply(any());
    return unresolvedSignature;
  }

  private static class FakeFunctionExpression extends FunctionExpression {

    public FakeFunctionExpression(FunctionName functionName, List<Expression> arguments) {
      super(functionName, arguments);
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      return null;
    }

    @Override
    public ExprType type() {
      return null;
    }

    @Override
    public String toString() {
      return getFunctionName().getFunctionName()
          + "(" + StringUtils.join(getArguments(), ", ") + ")";
    }
  }

}
