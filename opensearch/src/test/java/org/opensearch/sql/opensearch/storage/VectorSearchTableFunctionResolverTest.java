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
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
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

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> builder.apply(functionProperties, expressions));
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

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> builder.apply(functionProperties, expressions));
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

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("requires 4 arguments"));
  }

  @Test
  void resolve_rejectsPositionalArgument() {
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    // One positional literal mixed with three named arguments. Arity passes, but the resolver
    // must reject this before planning so the SQL layer returns a clean 400 rather than a 200
    // with zero rows.
    List<Expression> expressions =
        List.of(
            DSL.literal("my-index"),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0, 2.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    FunctionBuilder builder = resolver.resolve(functionSignature).getValue();

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("requires named arguments"));
  }

  @Test
  void resolve_rejectsDuplicateNamedArgument() {
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("table", DSL.literal("a")),
            DSL.namedArgument("table", DSL.literal("b")),
            DSL.namedArgument("vector", DSL.literal("[1.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    FunctionBuilder builder = resolver.resolve(functionSignature).getValue();

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("Duplicate argument name"));
    assertTrue(ex.getMessage().contains("table"));
  }

  @Test
  void resolverArityGuardRejectsExtraNamedArgument() {
    // The resolver's own builder runs validateArguments() first, so applying it directly to a
    // duplicate named argument plus all four required arguments (five total) throws a clean
    // ExpressionEvaluationException. This covers the resolver's arity guard only; the
    // repository-level cast path that originally crashed is covered by
    // resolveThroughRepositoryRejectsExtraNamedArgument and BuiltinFunctionRepositoryTest.
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("table", DSL.literal("a")),
            DSL.namedArgument("table", DSL.literal("b")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    FunctionBuilder builder = resolver.resolve(functionSignature).getValue();

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("requires 4 arguments"));
  }

  @Test
  void resolveThroughRepositoryRejectsExtraNamedArgument() {
    // Routes the vectorSearch resolver through BuiltinFunctionRepository the way production does
    // (storage-engine resolver registered as a datasource function). A duplicate named argument
    // plus all four required arguments gives five arguments against the resolver's fixed four-arg
    // signature, which exercises the cast-wrapper path that originally crashed with an unchecked
    // IndexOutOfBoundsException (surfacing as HTTP 500). It must now fail with a clean
    // ExpressionEvaluationException instead.
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    BuiltinFunctionRepository repository = BuiltinFunctionRepository.getInstance();
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("table", DSL.literal("a")),
            DSL.namedArgument("table", DSL.literal("b")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));

    assertThrows(
        ExpressionEvaluationException.class,
        () ->
            repository.compile(
                functionProperties,
                List.of(resolver),
                FunctionName.of("vectorsearch"),
                expressions));
  }

  @Test
  void resolve_rejectsUnknownArgumentName() {
    VectorSearchTableFunctionResolver resolver =
        new VectorSearchTableFunctionResolver(client, settings);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("table", DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0, 2.0]")),
            DSL.namedArgument("bogus", DSL.literal("k=5")));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    FunctionBuilder builder = resolver.resolve(functionSignature).getValue();

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> builder.apply(functionProperties, expressions));
    assertTrue(ex.getMessage().contains("Unknown argument name"));
    assertTrue(ex.getMessage().contains("bogus"));
  }
}
