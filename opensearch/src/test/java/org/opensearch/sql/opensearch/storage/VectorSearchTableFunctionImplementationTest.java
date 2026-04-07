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
  void testApplyArgumentsWithComplexOptions() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=10,method.ef_search=100");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
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
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "method.ef_search=100");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("one of k, max_distance, or min_score"));
  }

  @Test
  void testParseOptionsMultiple() {
    Map<String, String> opts =
        VectorSearchTableFunctionImplementation.parseOptions("k=5,method.ef_search=100");
    assertEquals("5", opts.get("k"));
    assertEquals("100", opts.get("method.ef_search"));
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
