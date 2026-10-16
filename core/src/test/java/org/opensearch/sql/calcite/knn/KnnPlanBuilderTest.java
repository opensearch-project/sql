/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.knn.KnnTestSchema.explain;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.sql.ast.KnnStep.ResolvedK;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.knn.KnnPlanBuilder.KnnRequest;
import org.opensearch.sql.exception.SemanticCheckException;

class KnnPlanBuilderTest {

  private static final List<Double> QUERY = List.of(0.12, 0.4, 0.33);

  private RelBuilder relBuilder;

  @BeforeEach
  void setUp() {
    CalcitePlanContext context = KnnTestSchema.newContext();
    relBuilder = context.relBuilder;
  }

  private static KnnRequest explicitK(int k) {
    return new KnnRequest(
        "embedding", QUERY, new KnnOptions(k, null, null), new ResolvedK(k, true));
  }

  private static KnnRequest derivedK(int k) {
    return new KnnRequest("embedding", QUERY, KnnOptions.NONE, new ResolvedK(k, false));
  }

  private String build(KnnRequest request) {
    return explain(KnnPlanBuilder.build(relBuilder, request).build());
  }

  private SemanticCheckException buildFails(KnnRequest request) {
    return assertThrows(
        SemanticCheckException.class, () -> KnnPlanBuilder.build(relBuilder, request));
  }

  @Test
  void explicitKIsTheFetchAndAMapOption() {
    relBuilder.scan("products");
    assertEquals(
        "LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[50])\n"
            + "  LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL), MAP('k', 50))])\n"
            + "    LogicalTableScan(table=[[products]])\n",
        build(explicitK(50)));
  }

  @Test
  void derivedKIsOnlyTheFetch() {
    relBuilder.scan("products");
    assertEquals(
        "LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "  LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL))])\n"
            + "    LogicalTableScan(table=[[products]])\n",
        build(derivedK(10)));
  }

  @Test
  void optionsAreEncodedInCanonicalOrder() {
    relBuilder.scan("products");
    KnnRequest request =
        new KnnRequest("embedding", QUERY, new KnnOptions(20, 100, 4), new ResolvedK(20, true));
    assertEquals(
        "LogicalSort(sort0=[$6], dir0=[DESC-nulls-last], fetch=[20])\n"
            + "  LogicalProject(doc_id=[$0], name=[$1], price=[$2], category=[$3],"
            + " embedding=[$4], _id=[$5], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL), MAP('k', 20), MAP('ef_search', 100), MAP('nprobes', 4))])\n"
            + "    LogicalTableScan(table=[[products]])\n",
        build(request));
  }

  @Test
  void scoreReplacesTheScanScoreColumnExactlyOnce() {
    relBuilder.scan("products");
    RelNode plan = KnnPlanBuilder.build(relBuilder, derivedK(10)).build();
    List<String> names = plan.getRowType().getFieldNames();
    assertEquals(
        List.of("doc_id", "name", "price", "category", "embedding", "_id", "_score"), names);
  }

  @Test
  void preFilterAndReferenceOnlyProjectionsMayPrecede() {
    relBuilder.scan("products");
    relBuilder.filter(relBuilder.equals(relBuilder.field("category"), relBuilder.literal("shoes")));
    relBuilder.project(
        relBuilder.field("doc_id"), relBuilder.field("embedding"), relBuilder.field("_score"));
    assertEquals(
        "LogicalSort(sort0=[$2], dir0=[DESC-nulls-last], fetch=[5])\n"
            + "  LogicalProject(doc_id=[$0], embedding=[$4], _score=[KNN_SCORE($4,"
            + " ARRAY(0.12E0:REAL, 0.4E0:REAL, 0.33E0:REAL))])\n"
            + "    LogicalFilter(condition=[=($3, 'shoes')])\n"
            + "      LogicalTableScan(table=[[products]])\n",
        build(derivedK(5)));
  }

  @Test
  void computedColumnsBeforeTheStepAreAPlacementError() {
    relBuilder.scan("products");
    relBuilder.projectPlus(
        relBuilder.alias(
            relBuilder.call(
                SqlStdOperatorTable.PLUS, relBuilder.field("price"), relBuilder.literal(1)),
            "z"));
    assertEquals(KnnErrors.KNN_PLACEMENT_MESSAGE, buildFails(explicitK(10)).getMessage());
  }

  @Test
  void aLimitBeforeTheStepIsAPlacementError() {
    relBuilder.scan("products").limit(0, 100);
    assertEquals(KnnErrors.KNN_PLACEMENT_MESSAGE, buildFails(explicitK(10)).getMessage());
  }

  @Test
  void anAggregateBeforeTheStepIsAPlacementError() {
    relBuilder.scan("products");
    relBuilder.aggregate(
        relBuilder.groupKey(relBuilder.field("embedding")), relBuilder.count(false, "c"));
    assertEquals(KnnErrors.KNN_PLACEMENT_MESSAGE, buildFails(explicitK(10)).getMessage());
  }

  @Test
  void aPreFilterWithASubsearchIsNotPushable() {
    RelNode docIds = relBuilder.scan("docs").project(relBuilder.field("doc_id")).build();
    relBuilder.scan("products");
    relBuilder.filter(RexSubQuery.in(docIds, ImmutableList.of(relBuilder.field("doc_id"))));
    assertEquals(
        "pre-filter [IN(doc_id, [subsearch])] cannot be pushed into the k-NN filter; move it after"
            + " knn as a post-filter",
        buildFails(explicitK(10)).getMessage());
  }

  @Test
  void knnScoreIsFoundAnywhereInAPlan() {
    relBuilder.scan("products");
    RelNode plan =
        KnnPlanBuilder.build(relBuilder, explicitK(5)).project(relBuilder.field("doc_id")).build();
    assertTrue(KnnPlanBuilder.containsKnnScore(plan));
    assertFalse(KnnPlanBuilder.containsKnnScore(relBuilder.scan("products").build()));
  }

  /** A node shared 30 levels deep is visited once, not 2^30 times. */
  @Test
  void knnScoreSearchVisitsSharedNodesOnce() {
    relBuilder.scan("products");
    RelNode knnStep = KnnPlanBuilder.build(relBuilder, explicitK(2)).build();
    RelNode plain = relBuilder.scan("docs").build();
    RelNode withKnn = knnStep;
    for (int level = 0; level < 30; level++) {
      plain = relBuilder.push(plain).push(plain).union(true).build();
      withKnn = relBuilder.push(withKnn).push(withKnn).union(true).build();
    }
    RelNode sharedPlain = plain;
    RelNode sharedKnn = withKnn;
    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          assertFalse(KnnPlanBuilder.containsKnnScore(sharedPlain));
          assertTrue(KnnPlanBuilder.containsKnnScore(sharedKnn));
        });
  }

  /** {@code where doc_id in [ ... | knn ... ]} and a scalar subsearch over a k-NN step. */
  @Test
  void knnScoreIsFoundInSubsearchPlans() {
    relBuilder.scan("products");
    RelNode knnIds =
        KnnPlanBuilder.build(relBuilder, explicitK(2)).project(relBuilder.field("doc_id")).build();
    RelNode inSubsearch =
        relBuilder
            .scan("docs")
            .filter(RexSubQuery.in(knnIds, ImmutableList.of(relBuilder.field("doc_id"))))
            .build();
    assertTrue(KnnPlanBuilder.containsKnnScore(inSubsearch));

    relBuilder.scan("products");
    RelNode maxScore =
        KnnPlanBuilder.build(relBuilder, explicitK(2))
            .aggregate(relBuilder.groupKey(), relBuilder.max("m", relBuilder.field("_score")))
            .build();
    RelNode scalarSubsearch =
        relBuilder
            .scan("docs")
            .project(relBuilder.field("doc_id"), RexSubQuery.scalar(maxScore))
            .build();
    assertTrue(KnnPlanBuilder.containsKnnScore(scalarSubsearch));
  }

  /** The k-NN score passed through, as the k-NN step builds it, then a projection of it. */
  @Test
  void theKnnScoreIsAKnnScoreColumn() {
    relBuilder.scan("products");
    RelNode plan = KnnPlanBuilder.build(relBuilder, derivedK(10)).build();
    assertTrue(KnnPlanBuilder.isKnnScoreColumn(plan, 6));
    RelNode projected = relBuilder.push(plan).project(relBuilder.field("_score")).build();
    assertTrue(KnnPlanBuilder.isKnnScoreColumn(projected, 0));
  }

  /** KNN_SCORE is REAL: a k-NN score cast to another type is no longer the k-NN score. */
  @Test
  void aKnnScoreOfAnotherTypeIsNot() {
    relBuilder.scan("products");
    RelNode plan = KnnPlanBuilder.build(relBuilder, derivedK(10)).build();
    RelNode cast =
        relBuilder
            .push(plan)
            .project(
                List.of(relBuilder.cast(relBuilder.field("_score"), SqlTypeName.DOUBLE)),
                List.of("_score"))
            .build();
    assertFalse(KnnPlanBuilder.isKnnScoreColumn(cast, 0));
  }

  /** The scan's own _score, and a column unnested from an array, are not the k-NN score. */
  @Test
  void theScanScoreAndAnUnnestedColumnAreNot() {
    RelNode scan = relBuilder.scan("products").build();
    assertFalse(KnnPlanBuilder.isKnnScoreColumn(scan, 6));
    RelNode unnested =
        relBuilder
            .values(new String[] {"a"}, 1.0f)
            .project(
                relBuilder.call(
                    SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                    relBuilder.cast(relBuilder.field(0), SqlTypeName.REAL)))
            .uncollect(List.of(), false)
            .build();
    assertTrue(unnested instanceof Uncollect, unnested::explain);
    assertFalse(KnnPlanBuilder.isKnnScoreColumn(unnested, 0));
  }

  /** A plan node the walk does not know is not given the benefit of the doubt. */
  @Test
  void aColumnOfAnUnknownNodeIsNot() {
    relBuilder.scan("products");
    RelNode plan = KnnPlanBuilder.build(relBuilder, derivedK(10)).build();
    RelNode unknown =
        new SingleRel(plan.getCluster(), plan.getTraitSet(), plan) {
          @Override
          protected RelDataType deriveRowType() {
            return getInput().getRowType();
          }
        };
    assertFalse(KnnPlanBuilder.isKnnScoreColumn(unknown, 6));
  }

  @Test
  void missingFieldIsNotAVectorField() {
    relBuilder.scan("products");
    KnnRequest request = new KnnRequest("vec", QUERY, KnnOptions.NONE, new ResolvedK(10, false));
    assertEquals("knn field [vec] is not a knn_vector field", buildFails(request).getMessage());
  }

  @Test
  void nonVectorFieldIsNotAVectorField() {
    relBuilder.scan("products");
    KnnRequest request = new KnnRequest("name", QUERY, KnnOptions.NONE, new ResolvedK(10, false));
    assertEquals("knn field [name] is not a knn_vector field", buildFails(request).getMessage());
  }

  @Test
  void computedVectorIsNotAVectorField() {
    relBuilder.scan("products");
    RelNode scan = relBuilder.build();
    RexNode vector = relBuilder.getRexBuilder().makeInputRef(scan, 4);
    RexNode computed = relBuilder.getRexBuilder().makeAbstractCast(vector.getType(), vector, false);
    relBuilder.push(
        LogicalProject.create(scan, List.of(), List.of(computed), List.of("v"), Set.of()));
    KnnRequest request = new KnnRequest("v", QUERY, KnnOptions.NONE, new ResolvedK(10, false));
    assertEquals("knn field [v] is not a knn_vector field", buildFails(request).getMessage());
  }

  @Test
  void renamedVectorIsStillAScanColumn() {
    relBuilder.scan("products");
    relBuilder.project(List.of(relBuilder.field("embedding")), List.of("v"), true);
    KnnRequest request = new KnnRequest("v", QUERY, KnnOptions.NONE, new ResolvedK(3, false));
    assertEquals(
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last], fetch=[3])\n"
            + "  LogicalProject(v=[$4], _score=[KNN_SCORE($4, ARRAY(0.12E0:REAL, 0.4E0:REAL,"
            + " 0.33E0:REAL))])\n"
            + "    LogicalTableScan(table=[[products]])\n",
        build(request));
  }

  @Test
  void binaryVectorsAreNotSupported() {
    relBuilder.scan("binary_vectors");
    KnnRequest request =
        new KnnRequest("embedding", List.of(1, 2), KnnOptions.NONE, new ResolvedK(1, false));
    assertEquals(KnnErrors.BINARY_VECTORS_NOT_SUPPORTED_MESSAGE, buildFails(request).getMessage());
  }

  @Test
  void unknownDimensionIsNotAVectorField() {
    relBuilder.scan("model_vectors");
    assertEquals(
        "knn field [embedding] is not a knn_vector field with a dimension in its mapping (e.g."
            + " it is model-based); knn needs the dimension",
        buildFails(derivedK(10)).getMessage());
  }

  /**
   * <i>vector mapping mismatch</i>: the indices of the query map the field with a different
   * dimension, data type or space type. It is reported before the dimension check, as a merged
   * dimension is unknown.
   */
  @ParameterizedTest
  @ValueSource(strings = {"dimension_mismatch", "data_type_mismatch", "space_type_mismatch"})
  void vectorMappingMismatchNamesTheIndices(String table) {
    relBuilder.scan(table);
    SemanticCheckException e = buildFails(derivedK(10));
    assertEquals("indices [a, b] have different vector mappings for [embedding]", e.getMessage());
    assertTrue(SemanticCheckException.preventsCalciteFallback(e));
  }

  @Test
  void vectorMappingMismatchWithoutIndexNames() {
    relBuilder.scan("unnamed_mismatch");
    assertEquals(
        "the indices of the query have different vector mappings for [embedding]",
        buildFails(derivedK(10)).getMessage());
  }

  @Test
  void dimensionMismatch() {
    relBuilder.scan("products");
    KnnRequest request =
        new KnnRequest("embedding", List.of(1.0, 2.0), KnnOptions.NONE, new ResolvedK(10, false));
    assertEquals(
        "vector has [2] dimensions; field [embedding] has [3]", buildFails(request).getMessage());
  }

  @Test
  void byteVectorsAcceptIntegersInRange() {
    relBuilder.scan("byte_vectors");
    KnnRequest request =
        new KnnRequest(
            "embedding",
            List.of(-128, new BigDecimal("127.0"), 0L),
            KnnOptions.NONE,
            new ResolvedK(2, false));
    assertEquals(
        "LogicalSort(sort0=[$3], dir0=[DESC-nulls-last], fetch=[2])\n"
            + "  LogicalProject(doc_id=[$0], embedding=[$1], _id=[$2], _score=[KNN_SCORE($1,"
            + " ARRAY(-128.0E0:REAL, 127.0E0:REAL, 0.0E0:REAL))])\n"
            + "    LogicalTableScan(table=[[byte_vectors]])\n",
        build(request));
  }

  @Test
  void byteVectorsRejectElementsOutOfRange() {
    relBuilder.scan("byte_vectors");
    KnnRequest outOfRange =
        new KnnRequest("embedding", List.of(1, 128, 0), KnnOptions.NONE, new ResolvedK(2, false));
    assertEquals(
        "knn field [embedding] holds byte vectors; vector element 1 [128] must be an integer in"
            + " [-128, 127]",
        buildFails(outOfRange).getMessage());
    KnnRequest fraction =
        new KnnRequest("embedding", List.of(1, 2, 0.5), KnnOptions.NONE, new ResolvedK(2, false));
    assertEquals(
        "knn field [embedding] holds byte vectors; vector element 2 [0.5] must be an integer in"
            + " [-128, 127]",
        buildFails(fraction).getMessage());
  }

  @Test
  void nonFiniteElementsAreInvalid() {
    relBuilder.scan("products");
    KnnRequest request =
        new KnnRequest(
            "embedding", List.of(1.0, Double.NaN, 2.0), KnnOptions.NONE, new ResolvedK(2, false));
    assertEquals(
        "knn vector must be a non-empty array of numeric literals, got [[1.0, NaN, 2.0]]",
        buildFails(request).getMessage());
  }

  @Test
  void kOutOfRange() {
    relBuilder.scan("products");
    assertEquals("k must be between 1 and 10000, got [0]", buildFails(derivedK(0)).getMessage());
    assertEquals(
        "k must be between 1 and 10000, got [10001]", buildFails(derivedK(10001)).getMessage());
  }

  @Test
  void requestRejectsAResolvedKThatContradictsTheOptions() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new KnnRequest("e", QUERY, new KnnOptions(5, null, null), new ResolvedK(5, false)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new KnnRequest("e", QUERY, new KnnOptions(5, null, null), new ResolvedK(6, true)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new KnnRequest("e", QUERY, KnnOptions.NONE, new ResolvedK(5, true)));
  }
}
