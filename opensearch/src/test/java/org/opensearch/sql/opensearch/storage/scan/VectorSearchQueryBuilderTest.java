/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.FilterType;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalValues;

class VectorSearchQueryBuilderTest {

  @Test
  void knnQuerySetAsScoringQuery() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");

    new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    QueryBuilder query = requestBuilder.getSourceBuilder().query();
    assertTrue(
        query instanceof WrapperQueryBuilder,
        "knn query should be set directly as top-level query (scoring context)");
  }

  @Test
  void pushDownFilterKeepsKnnInScoringContext() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    // Simulate WHERE name = 'John'
    var condition = DSL.equal(new ReferenceExpression("name", STRING), DSL.literal("John"));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    boolean pushed = builder.pushDownFilter(filter);

    assertTrue(pushed, "pushDownFilter should succeed");
    QueryBuilder resultQuery = requestBuilder.getSourceBuilder().query();
    assertTrue(resultQuery instanceof BoolQueryBuilder, "Result should be a BoolQuery");
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) resultQuery;
    assertEquals(1, boolQuery.must().size(), "knn query should be in must (scoring context)");
    assertEquals(1, boolQuery.filter().size(), "WHERE predicate should be in filter (non-scoring)");
    assertTrue(
        boolQuery.must().get(0) instanceof WrapperQueryBuilder,
        "must clause should contain the original knn WrapperQueryBuilder");
  }

  @Test
  void pushDownLimitWithinKSucceeds() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var limit = new LogicalLimit(dummyChild, 3, 0);

    boolean pushed = builder.pushDownLimit(limit);
    assertTrue(pushed, "LIMIT within k should succeed");
  }

  @Test
  void pushDownLimitExceedingKThrows() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var limit = new LogicalLimit(dummyChild, 10, 0);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownLimit(limit));
    assertTrue(ex.getMessage().contains("LIMIT 10 exceeds k=5"));
  }

  @Test
  void pushDownLimitEqualToKSucceeds() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var limit = new LogicalLimit(dummyChild, 5, 0);

    boolean pushed = builder.pushDownLimit(limit);
    assertTrue(pushed, "LIMIT equal to k should succeed");
  }

  @Test
  void pushDownLimitRadialModeNoRestriction() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("max_distance", "10.0"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var limit = new LogicalLimit(dummyChild, 100, 0);

    boolean pushed = builder.pushDownLimit(limit);
    assertTrue(pushed, "Radial mode should not restrict LIMIT");
  }

  @Test
  void pushDownLimitMinScoreModeNoRestriction() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("min_score", "0.5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var limit = new LogicalLimit(dummyChild, 100, 0);

    boolean pushed = builder.pushDownLimit(limit);
    assertTrue(pushed, "min_score mode should not restrict LIMIT");
  }

  @Test
  void pushDownSortScoreDescAccepted() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC,
                    new ReferenceExpression("_score", ExprCoreType.FLOAT))));

    boolean pushed = builder.pushDownSort(sort);
    assertTrue(pushed, "ORDER BY _score DESC should be accepted");
  }

  @Test
  void pushDownSortPreservesSortCountAsLimit() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "10"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    // LogicalSort with count=7 simulates a sort+limit combined node (PPL path)
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            7,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC,
                    new ReferenceExpression("_score", ExprCoreType.FLOAT))));

    boolean pushed = builder.pushDownSort(sort);
    assertTrue(pushed, "ORDER BY _score DESC with count should be accepted");
    assertEquals(
        7,
        requestBuilder.getMaxResponseSize(),
        "sort.getCount() should be pushed down as request size");
  }

  @Test
  void pushDownSortCountExceedingKRejects() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    // LogicalSort with count=10 exceeds k=5 — should be rejected
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            10,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC,
                    new ReferenceExpression("_score", ExprCoreType.FLOAT))));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownSort(sort));
    assertTrue(ex.getMessage().contains("LIMIT 10 exceeds k=5"));
  }

  @Test
  void pushDownSortNonScoreFieldRejected() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC,
                    new ReferenceExpression("name", STRING))));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownSort(sort));
    assertTrue(ex.getMessage().contains("unsupported sort expression"));
  }

  @Test
  void pushDownSortMultipleExpressionsRejectsNonScore() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC,
                    new ReferenceExpression("_score", ExprCoreType.FLOAT)),
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC,
                    new ReferenceExpression("name", STRING))));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownSort(sort));
    assertTrue(ex.getMessage().contains("unsupported sort expression"));
  }

  @Test
  void pushDownSortScoreAscRejected() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC,
                    new ReferenceExpression("_score", ExprCoreType.FLOAT))));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownSort(sort));
    assertTrue(ex.getMessage().contains("_score ASC is not supported"));
  }

  @Test
  void pushDownFilterCompoundPredicateSurvives() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    // Simulate WHERE name = 'John' AND age > 30
    var condition =
        DSL.and(
            DSL.equal(new ReferenceExpression("name", STRING), DSL.literal("John")),
            DSL.greater(new ReferenceExpression("age", ExprCoreType.INTEGER), DSL.literal(30)));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    boolean pushed = builder.pushDownFilter(filter);

    assertTrue(pushed, "pushDownFilter with compound predicate should succeed");
    QueryBuilder resultQuery = requestBuilder.getSourceBuilder().query();
    assertTrue(resultQuery instanceof BoolQueryBuilder, "Result should be a BoolQuery");
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) resultQuery;
    assertEquals(1, boolQuery.must().size(), "knn query should be in must (scoring context)");
    assertEquals(1, boolQuery.filter().size(), "compound WHERE should be in filter (non-scoring)");
  }

  @Test
  void pushDownFilterEfficientPlacesInsideKnn() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    // Callback simulates VectorSearchIndex rebuilding knn with filter
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder("{\"knn\":{\"filter\":\"embedded\"}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.EFFICIENT, true, rebuildWithFilter);

    var condition = DSL.equal(new ReferenceExpression("city", STRING), DSL.literal("Miami"));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    boolean pushed = builder.pushDownFilter(filter);

    assertTrue(pushed, "pushDownFilter should succeed");
    QueryBuilder resultQuery = requestBuilder.getSourceBuilder().query();
    assertTrue(
        resultQuery instanceof WrapperQueryBuilder,
        "Efficient filter should produce a WrapperQueryBuilder (rebuilt knn), not BoolQuery");
  }

  @Test
  void pushDownFilterExplicitPostProducesBool() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.POST, true, null);

    var condition = DSL.equal(new ReferenceExpression("name", STRING), DSL.literal("John"));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    boolean pushed = builder.pushDownFilter(filter);

    assertTrue(pushed);
    QueryBuilder resultQuery = requestBuilder.getSourceBuilder().query();
    assertTrue(resultQuery instanceof BoolQueryBuilder);
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) resultQuery;
    assertEquals(1, boolQuery.must().size());
    assertEquals(1, boolQuery.filter().size());
  }

  // ── Build-time validation ────────────────────────────────────────────

  @Test
  void buildRejectsExplicitFilterTypePostWithoutWhere() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.POST, true, null);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, builder::build);
    assertTrue(ex.getMessage().contains("filter_type requires a pushdownable WHERE clause"));
  }

  @Test
  void buildRejectsExplicitFilterTypeEfficientWithoutWhere() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder("{\"knn\":{\"filter\":\"embedded\"}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.EFFICIENT, true, rebuildWithFilter);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, builder::build);
    assertTrue(ex.getMessage().contains("filter_type requires a pushdownable WHERE clause"));
  }

  @Test
  void buildSucceedsWithNoFilterTypeAndNoWhere() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    OpenSearchRequestBuilder result = builder.build();
    assertNotNull(result);
  }

  @Test
  void buildSucceedsWithFilterTypeAndPushedWhere() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.POST, true, null);

    var condition = DSL.equal(new ReferenceExpression("name", STRING), DSL.literal("John"));
    var dummyChild = new LogicalValues(Collections.emptyList());
    builder.pushDownFilter(new LogicalFilter(dummyChild, condition));

    OpenSearchRequestBuilder result = builder.build();
    assertNotNull(result);
  }

  // ── Regression: LIMIT and sort invariants under efficient mode ──────

  @Test
  void pushDownLimitExceedingKThrowsUnderEfficientMode() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.EFFICIENT, true, rebuildWithFilter);

    var dummyChild = new LogicalValues(Collections.emptyList());
    var limit = new LogicalLimit(dummyChild, 10, 0);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownLimit(limit));
    assertTrue(ex.getMessage().contains("LIMIT 10 exceeds k=5"));
  }

  @Test
  void pushDownSortScoreDescAcceptedUnderEfficientMode() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.EFFICIENT, true, rebuildWithFilter);

    var dummyChild = new LogicalValues(Collections.emptyList());
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC,
                    new ReferenceExpression("_score", ExprCoreType.FLOAT))));

    boolean pushed = builder.pushDownSort(sort);
    assertTrue(pushed, "ORDER BY _score DESC should be accepted under efficient mode");
  }

  @Test
  void pushDownSortNonScoreRejectedUnderEfficientMode() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"),
            FilterType.EFFICIENT, true, rebuildWithFilter);

    var dummyChild = new LogicalValues(Collections.emptyList());
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC,
                    new ReferenceExpression("name", STRING))));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownSort(sort));
    assertTrue(ex.getMessage().contains("unsupported sort expression"));
  }

  private OpenSearchRequestBuilder createRequestBuilder() {
    return new OpenSearchRequestBuilder(
        mock(OpenSearchExprValueFactory.class), 10000, mock(Settings.class));
  }
}
