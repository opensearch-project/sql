/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@ExtendWith(MockitoExtension.class)
class VectorSearchTableFunctionResolverTest {

  @Mock private OpenSearchClient client;

  @Mock private Settings settings;

  @Mock private FunctionProperties functionProperties;

  @Test
  void testResolve() {
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("table", DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0, 2.0, 3.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));

    Pair<FunctionSignature, FunctionBuilder> resolution = resolver.resolve(functionSignature);

    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, resolver.getFunctionName());
    assertEquals(List.of(STRING, STRING, STRING, STRING), resolution.getKey().getParamTypeList());

    TableFunctionImplementation impl =
        (TableFunctionImplementation) resolution.getValue().apply(functionProperties, expressions);
    assertTrue(impl instanceof VectorSearchTableFunctionImplementation);
  }

  @Test
  void testWrongArgumentCount() {
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("table", DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));

    Pair<FunctionSignature, FunctionBuilder> resolution = resolver.resolve(functionSignature);
    FunctionBuilder builder = resolution.getValue();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("requires 4 arguments"));
  }

  @Test
  void testTooManyArguments() {
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("table", DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")),
            DSL.namedArgument("extra", DSL.literal("unexpected")));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));

    Pair<FunctionSignature, FunctionBuilder> resolution = resolver.resolve(functionSignature);
    FunctionBuilder builder = resolution.getValue();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("requires 4 arguments"));
  }

  @Test
  void testZeroArguments() {
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> expressions = List.of();
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));

    Pair<FunctionSignature, FunctionBuilder> resolution = resolver.resolve(functionSignature);
    FunctionBuilder builder = resolution.getValue();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("requires 4 arguments"));
  }
}
