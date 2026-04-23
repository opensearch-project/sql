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
import org.opensearch.sql.opensearch.storage.capability.KnnPluginCapability;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class VectorSearchTableFunctionImplementationTest {

  @Mock private OpenSearchClient client;

  @Mock private Settings settings;

  // No-op capability — tests in this class don't exercise the k-NN plugin probe.
  // Dedicated tests for the probe live in KnnPluginCapabilityTest.
  private final KnnPluginCapability knnCapability =
      org.mockito.Mockito.mock(KnnPluginCapability.class);

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
  void testApplyArgumentsDoesNotProbeKnnCapability() {
    // Contract: applyArguments() runs during analysis (including _explain) and must NOT invoke
    // the k-NN plugin probe. The probe is deferred to scan open() so pluginless clusters can
    // still explain and validate vectorSearch() queries locally.
    KnnPluginCapability observingCapability = org.mockito.Mockito.mock(KnnPluginCapability.class);
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args =
        List.of(
            DSL.namedArgument("table", DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0, 2.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(
            functionName, args, client, settings, observingCapability);
    impl.applyArguments();
    org.mockito.Mockito.verify(observingCapability, org.mockito.Mockito.never()).requireInstalled();
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
  void testUnknownOptionKeyOnlyThrows() {
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
  void testNoRequiredOptionThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Missing required option"));
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
        new VectorSearchTableFunctionImplementation(
            functionName, args, client, settings, knnCapability);
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
  void testMutualExclusivityKAndMaxDistanceThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=5,max_distance=10.0");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Only one of"));
  }

  @Test
  void testMutualExclusivityKAndMinScoreThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=5,min_score=0.5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Only one of"));
  }

  @Test
  void testMutualExclusivityAllThreeThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs(
            "my-index", "embedding", "[1.0, 2.0]", "k=5,max_distance=10.0,min_score=0.5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Only one of"));
  }

  @Test
  void testKTooSmallThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=0");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("k must be between 1 and 10000"));
  }

  @Test
  void testKTooLargeThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=10001");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("k must be between 1 and 10000"));
  }

  @Test
  void testKBoundaryValuesAllowed() {
    // k=1 should work
    VectorSearchTableFunctionImplementation impl1 =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=1");
    assertTrue(impl1.applyArguments() instanceof VectorSearchIndex);

    // k=10000 should work
    VectorSearchTableFunctionImplementation impl2 =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=10000");
    assertTrue(impl2.applyArguments() instanceof VectorSearchIndex);
  }

  @Test
  void testNonNamedArgThrows() {
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args = List.of(DSL.literal("my-index"));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(
            functionName, args, client, settings, knnCapability);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("requires named arguments"));
  }

  @Test
  void testNullArgNameThrows() {
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args =
        List.of(
            DSL.namedArgument(null, DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0, 2.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(
            functionName, args, client, settings, knnCapability);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("requires named arguments"));
  }

  @Test
  void testNaNVectorComponentThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, NaN, 3.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must be a finite number"));
  }

  @Test
  void testEmptyOptionKeyThrows() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("=value"));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
  }

  @Test
  void testEmptyOptionValueThrows() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("key="));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
  }

  @Test
  void testNegativeKThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=-1");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("k must be between 1 and 10000"));
  }

  @Test
  void testNaNMaxDistanceThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "max_distance=NaN");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must be a finite number"));
  }

  @Test
  void testNaNMinScoreThrows() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "min_score=NaN");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("must be a finite number"));
  }

  @Test
  void testCaseInsensitiveArgLookup() {
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args =
        List.of(
            DSL.namedArgument("TABLE", DSL.literal("my-index")),
            DSL.namedArgument("FIELD", DSL.literal("embedding")),
            DSL.namedArgument("VECTOR", DSL.literal("[1.0, 2.0]")),
            DSL.namedArgument("OPTION", DSL.literal("k=5")));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(
            functionName, args, client, settings, knnCapability);
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testInvalidFilterTypeRejects() {
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args =
        List.of(
            DSL.namedArgument("table", DSL.literal("my-index")),
            DSL.namedArgument("field", DSL.literal("embedding")),
            DSL.namedArgument("vector", DSL.literal("[1.0, 2.0]")),
            DSL.namedArgument("option", DSL.literal("k=5,filter_type=invalid")));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(
            functionName, args, client, settings, knnCapability);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, impl::applyArguments);
    assertTrue(ex.getMessage().contains("filter_type must be one of"));
  }

  @Test
  void testFilterTypePostAccepted() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=5,filter_type=post");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testFilterTypeEfficientAccepted() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=5,filter_type=efficient");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void testParseOptionsPreservesFilterTypeValue() {
    Map<String, String> options =
        VectorSearchTableFunctionImplementation.parseOptions("k=5,filter_type=post");
    assertEquals("post", options.get("filter_type"));
  }

  @Test
  void applyArguments_rejectsInvalidTableName() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("idx\"; DROP", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid table name"));
    assertTrue(
        ex.getMessage()
            .contains("must contain only alphanumeric characters, dots, underscores, or hyphens"));
  }

  @Test
  void applyArguments_rejectsAllRoutingTarget() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("_all", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid table name"));
    assertTrue(ex.getMessage().contains("_all"));
  }

  @Test
  void applyArguments_rejectsSingleDotTable() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs(".", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid table name"));
  }

  @Test
  void applyArguments_rejectsDoubleDotTable() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("..", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid table name"));
  }

  @Test
  void applyArguments_rejectsWildcardTableWithDedicatedMessage() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("sql_vector_*", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid table name"));
    assertTrue(ex.getMessage().contains("wildcards ('*')"));
    assertTrue(ex.getMessage().contains("single concrete index"));
  }

  @Test
  void applyArguments_rejectsBareStarTableWithDedicatedMessage() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("*", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("wildcards ('*')"));
  }

  @Test
  void applyArguments_rejectsMultiTargetTableWithDedicatedMessage() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("idx_a,idx_b", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid table name"));
    assertTrue(ex.getMessage().contains("multi-target"));
    assertTrue(ex.getMessage().contains("single concrete index"));
  }

  @Test
  void applyArguments_rejectsMidNameStarTable() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("foo*bar", "embedding", "[1.0, 2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("wildcards ('*')"));
  }

  @Test
  void validateNamedArgs_rejectsDuplicateNames() {
    // Two occurrences of "table" reach the Implementation layer directly (bypassing the resolver).
    FunctionName functionName = FunctionName.of("vectorsearch");
    List<Expression> args =
        List.of(
            DSL.namedArgument("table", DSL.literal("a")),
            DSL.namedArgument("table", DSL.literal("b")),
            DSL.namedArgument("vector", DSL.literal("[1.0]")),
            DSL.namedArgument("option", DSL.literal("k=5")));
    VectorSearchTableFunctionImplementation impl =
        new VectorSearchTableFunctionImplementation(
            functionName, args, client, settings, knnCapability);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Duplicate argument name"));
    assertTrue(ex.getMessage().contains("table"));
  }

  // ── Option parsing: empty value, whitespace, unknown keys ────────────

  @Test
  void parseOptions_rejectsEmptyValue() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("k="));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
  }

  @Test
  void parseOptions_rejectsEmptyValueInMidSegment() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("k=,filter_type=post"));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
  }

  @Test
  void parseOptions_trimsWhitespaceAroundKeyAndValue() {
    Map<String, String> options =
        VectorSearchTableFunctionImplementation.parseOptions(" k = 5 , filter_type = post ");
    assertEquals("5", options.get("k"));
    assertEquals("post", options.get("filter_type"));
  }

  @Test
  void applyArguments_rejectsUnknownOptionKey() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs(
            "my-index", "embedding", "[1.0, 2.0]", "k=5,method_parameters.ef_search=100");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Unknown option key"));
    assertTrue(ex.getMessage().contains("method_parameters.ef_search"));
  }

  // ── Vector parsing: non-comma separator ─────────────────────────────

  @Test
  void applyArguments_rejectsSemicolonSeparatorInVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0;2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("vector="));
    assertTrue(ex.getMessage().contains("comma-separated"));
  }

  @Test
  void applyArguments_rejectsColonSeparatorInVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0:2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("vector="));
  }

  @Test
  void applyArguments_rejectsPipeSeparatorInVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0|2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("vector="));
  }

  // ── Option bounds: negative k, min_score, max_distance ──────────────

  @Test
  void applyArguments_negativeKMessageCitesRange() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=-3");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("1"));
    assertTrue(ex.getMessage().contains("10000"));
  }

  @Test
  void applyArguments_rejectsNegativeMinScore() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "min_score=-0.5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("min_score"));
    assertTrue(ex.getMessage().contains("non-negative"));
  }

  @Test
  void applyArguments_rejectsNegativeMaxDistance() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "max_distance=-1.0");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("max_distance"));
    assertTrue(ex.getMessage().contains("non-negative"));
  }

  @Test
  void applyArguments_acceptsZeroMinScore() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "min_score=0");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  @Test
  void applyArguments_acceptsZeroMaxDistance() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "max_distance=0");
    Table table = impl.applyArguments();
    assertTrue(table instanceof VectorSearchIndex);
  }

  // ── Vector parsing: trailing / empty components (PR #5381 review) ─────

  @Test
  void applyArguments_rejectsTrailingCommaInVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0,2.0,]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid vector component"));
    assertTrue(ex.getMessage().contains("trailing or consecutive commas"));
  }

  @Test
  void applyArguments_rejectsConsecutiveCommasInVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0,,2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid vector component"));
    assertTrue(ex.getMessage().contains("trailing or consecutive commas"));
  }

  @Test
  void applyArguments_rejectsLeadingCommaInVector() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[,1.0,2.0]", "k=5");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    assertTrue(ex.getMessage().contains("Invalid vector component"));
  }

  // ── Option parsing: empty segments (PR #5381 review) ─────────────────

  @Test
  void parseOptions_rejectsTrailingEmptySegment() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("k=5,"));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
    assertTrue(ex.getMessage().contains("trailing or consecutive commas"));
  }

  @Test
  void parseOptions_rejectsLeadingEmptySegment() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions(",k=5"));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
  }

  @Test
  void parseOptions_rejectsConsecutiveCommas() {
    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchTableFunctionImplementation.parseOptions("k=5,,filter_type=post"));
    assertTrue(ex.getMessage().contains("Malformed option segment"));
  }

  // ── Unknown-key error lists supported keys in stable order (PR #5381 review) ──

  @Test
  void applyArguments_unknownOptionKeyErrorListsSupportedKeysInStableOrder() {
    VectorSearchTableFunctionImplementation impl =
        createImplWithArgs("my-index", "embedding", "[1.0, 2.0]", "k=5,bogus=1");
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> impl.applyArguments());
    // Match the rendered list literal (e.g. "[k, max_distance, min_score, filter_type]") rather
    // than searching for the substring "k", which would match the first "k" in "Unknown option
    // key" and reduce the assertion to a tautology.
    assertTrue(
        ex.getMessage().contains("[k, max_distance, min_score, filter_type]"),
        "expected stable key order in error; got: " + ex.getMessage());
  }

  @Test
  void parseOptions_emptyStringReturnsEmptyMap() {
    // The wholly empty option string is explicitly allowed through parseOptions so it flows to
    // the "Missing required option" gate in validateOptions. Pins that contract.
    Map<String, String> opts = VectorSearchTableFunctionImplementation.parseOptions("");
    assertTrue(opts.isEmpty());
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
    return new VectorSearchTableFunctionImplementation(
        functionName, args, client, settings, knnCapability);
  }
}
