/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class VectorSearchTableFunctionImplementationTest {

  @Mock private OpenSearchClient client;

  @Mock private Settings settings;

  @Test
  void testValueOfThrows() {
    VectorSearchTableFunctionImplementation impl = createImpl();
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> impl.valueOf());
    assertTrue(ex.getMessage().contains("only supported in FROM clause"));
  }

  @Test
  void testType() {
    VectorSearchTableFunctionImplementation impl = createImpl();
    assertEquals(ExprCoreType.STRUCT, impl.type());
  }

  @Test
  void testToString() {
    VectorSearchTableFunctionImplementation impl = createImpl();
    String str = impl.toString();
    assertTrue(str.contains("vectorsearch"));
    assertTrue(str.contains("table="));
    assertTrue(str.contains("my-index"));
  }

  @Test
  void testApplyArguments() {
    VectorSearchTableFunctionImplementation impl = createImpl();
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testApplyArgumentsWithBracketedVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0, 3.0]", "k=5");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testApplyArgumentsWithUnbracketedVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "1.0, 2.0, 3.0", "k=5");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testUnknownOptionKeyThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=10,method.ef_search=100");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Unknown option key"));
    assertTrue(ex.getMessage().contains("method.ef_search"));
  }

  @Test
  void testApplyArgumentsWithMaxDistance() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "max_distance=10.0");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testApplyArgumentsWithMinScore() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "min_score=0.5");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testMissingSearchModeOptionThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "not_a_key=100");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Unknown option key"));
  }

  @Test
  void testParseOptionsMultiple() {
    Map<String, String> opts =
        VectorSearchTableFunctionImplementation.parseOptions("k=5,max_distance=10.0");
    assertEquals("5", opts.get("k"));
    assertEquals("10.0", opts.get("max_distance"));
  }

  @Test
  void testMalformedOptionSegmentThrows() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("k=5,badoption"));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
  }

  @Test
  void testDuplicateOptionKeyThrows() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("k=5,k=10"));
    assertTrue(ex.getMessage().contains("Duplicate option key"));
  }

  @Test
  void testEmptyVectorThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must not be empty"));
  }

  @Test
  void testMalformedVectorComponentThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, abc, 3.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid vector component"));
  }

  @Test
  void testNonFiniteVectorComponentThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, Infinity, 3.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must be a finite number"));
  }

  @Test
  void testMissingArgumentThrows() {
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args =
        List.of(
            DSL.namedArgument("table", DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0, 2.0]")));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(functionName, args, client, settings);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertEquals("Missing required argument: option", ex.getMessage());
  }

  @Test
  void testInvalidFieldNameThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "field\"injection", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid field name"));
  }

  @Test
  void testNestedFieldNameAllowed() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "doc.embedding", "[1.0, 2.0]", "k=5");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testNonNumericKThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=abc");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must be an integer"));
  }

  @Test
  void testNonNumericMaxDistanceThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "max_distance=notanumber");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must be a number"));
  }

  @Test
  void testInfiniteMinScoreThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "min_score=Infinity");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must be a finite number"));
  }

  @Test
  void testNonNamedArgThrows() {
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args = List.of(DSL.literal("my-index"));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(functionName, args, client, settings);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("requires named arguments"));
  }

  private VectorSearchTableFunctionImplementation createImpl() {
    return createImplWithArgs("my-index", "embedding", "[1.0, 2.0, 3.0]", "k=5");
  }

  private VectorSearchTableFunctionImplementation createImplWithArgs(
      String table, String field, String vector, String option) {
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args =
        List.of(
            DSL.namedArgument("table", DSL.literal(table)),
            DSL.namedArgument("field", DSL.literal(field)),
            DSL.namedArgument("vector", DSL.literal(vector)),
            DSL.namedArgument("option", DSL.literal(option)));
    return new VectorSearchTableFunctionImplementation(functionName, args, client, settings);
  }
}
