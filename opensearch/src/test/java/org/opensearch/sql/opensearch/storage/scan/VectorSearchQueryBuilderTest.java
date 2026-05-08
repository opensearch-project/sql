/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
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
            requestBuilder,
            knnQuery,
            Map.of("k", "5"),
            FilterType.EFFICIENT,
            true,
            rebuildWithFilter);

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
            requestBuilder, knnQuery, Map.of("k", "5"), FilterType.POST, true, null);

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

  // ── Constructor validation ──────────────────────────────────────────

  @Test
  void constructorRejectsEfficientModeWithNullCallback() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new VectorSearchQueryBuilder(
                requestBuilder, knnQuery, Map.of("k", "5"), FilterType.EFFICIENT, true, null));
  }

  // ── Build-time validation ────────────────────────────────────────────

  @Test
  void buildRejectsExplicitFilterTypePostWithoutWhere() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"), FilterType.POST, true, null);

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
            requestBuilder,
            knnQuery,
            Map.of("k", "5"),
            FilterType.EFFICIENT,
            true,
            rebuildWithFilter);

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
            requestBuilder, knnQuery, Map.of("k", "5"), FilterType.POST, true, null);

    var condition = DSL.equal(new ReferenceExpression("name", STRING), DSL.literal("John"));
    var dummyChild = new LogicalValues(Collections.emptyList());
    builder.pushDownFilter(new LogicalFilter(dummyChild, condition));

    OpenSearchRequestBuilder result = builder.build();
    assertNotNull(result);
  }

  // ── Radial without LIMIT rejection ─────────────────────────────────

  @Test
  void buildRejectsRadialMaxDistanceWithoutLimit() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("max_distance", "10.0"));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, builder::build);
    assertTrue(ex.getMessage().contains("LIMIT is required for radial vector search"));
  }

  @Test
  void buildRejectsRadialMinScoreWithoutLimit() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("min_score", "0.5"));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, builder::build);
    assertTrue(ex.getMessage().contains("LIMIT is required for radial vector search"));
  }

  @Test
  void buildSucceedsRadialWithLimit() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("max_distance", "10.0"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    builder.pushDownLimit(new LogicalLimit(dummyChild, 50, 0));

    OpenSearchRequestBuilder result = builder.build();
    assertNotNull(result);
  }

  @Test
  void buildSucceedsTopKWithoutLimit() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

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
            requestBuilder,
            knnQuery,
            Map.of("k", "5"),
            FilterType.EFFICIENT,
            true,
            rebuildWithFilter);

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
            requestBuilder,
            knnQuery,
            Map.of("k", "5"),
            FilterType.EFFICIENT,
            true,
            rebuildWithFilter);

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
            requestBuilder,
            knnQuery,
            Map.of("k", "5"),
            FilterType.EFFICIENT,
            true,
            rebuildWithFilter);

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

  // ── Non-pushdownable filter handling ──────────────────────────────────

  @Test
  void pushDownFilterNonPushdownableWithExplicitFilterTypeThrows() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder, knnQuery, Map.of("k", "5"), FilterType.POST, true, null);

    // STRUCT = STRUCT triggers ScriptQueryUnSupportedException in FilterQueryBuilder
    var condition =
        DSL.equal(
            new ReferenceExpression("nested_field", ExprCoreType.STRUCT),
            new ReferenceExpression("other_field", ExprCoreType.STRUCT));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownFilter(filter));
    assertTrue(
        ex.getMessage().contains("filter_type only works when the WHERE clause can be translated"));
    assertTrue(ex.getMessage().contains("Rewrite the WHERE clause or omit filter_type"));
  }

  @Test
  void pushDownFilterNonPushdownableWithoutExplicitFilterTypeFallsBack() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    // STRUCT = STRUCT triggers ScriptQueryUnSupportedException in FilterQueryBuilder
    var condition =
        DSL.equal(
            new ReferenceExpression("nested_field", ExprCoreType.STRUCT),
            new ReferenceExpression("other_field", ExprCoreType.STRUCT));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    boolean pushed = builder.pushDownFilter(filter);
    assertFalse(pushed, "Non-pushdownable filter should return false for in-memory fallback");
  }

  // ── OFFSET rejection ────────────────────────────────────────────────

  @Test
  void pushDownLimit_rejectsNonZeroOffset() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    // LIMIT 3 OFFSET 2: the planner passes both through LogicalLimit
    var limit = new LogicalLimit(dummyChild, 3, 2);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownLimit(limit));
    assertTrue(
        ex.getMessage().contains("OFFSET is not supported on vectorSearch()"),
        "Expected OFFSET rejection message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("LIMIT only"),
        "Expected remediation guidance in message, got: " + ex.getMessage());
  }

  @Test
  void pushDownLimit_acceptsZeroOffset() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    var limit = new LogicalLimit(dummyChild, 3, 0);

    // Zero offset is the normal case; must continue to succeed.
    assertTrue(builder.pushDownLimit(limit));
  }

  // ── WHERE on _score rejection ────────────────────────────────────────

  @Test
  void pushDownFilter_rejectsScoreReferenceInWhere() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    // WHERE _score > 0.5 (note: _score is a synthetic column, not a stored field)
    var condition =
        DSL.greater(new ReferenceExpression("_score", ExprCoreType.FLOAT), DSL.literal(0.5));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownFilter(filter));
    assertTrue(
        ex.getMessage().contains("WHERE on _score is not supported"),
        "Expected _score rejection message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("min_score"),
        "Expected remediation guidance pointing at option='min_score=...', got: "
            + ex.getMessage());
  }

  @Test
  void pushDownFilter_rejectsScoreReferenceInsideCompound() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    // WHERE state = 'TX' AND _score > 0.5: rejection must walk compound predicates
    var condition =
        DSL.and(
            DSL.equal(new ReferenceExpression("state", STRING), DSL.literal("TX")),
            DSL.greater(new ReferenceExpression("_score", ExprCoreType.FLOAT), DSL.literal(0.5)));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownFilter(filter));
    assertTrue(
        ex.getMessage().contains("WHERE on _score is not supported"),
        "Expected _score rejection message, got: " + ex.getMessage());
  }

  @Test
  void pushDownFilter_rejectsUppercaseScoreReference() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    // WHERE _SCORE > 0.5 must be rejected the same way as _score; the check is case-insensitive
    // so variants that preserve original casing cannot bypass the guard.
    var condition =
        DSL.greater(new ReferenceExpression("_SCORE", ExprCoreType.FLOAT), DSL.literal(0.5));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownFilter(filter));
    assertTrue(
        ex.getMessage().contains("WHERE on _score is not supported"),
        "Expected _score rejection message, got: " + ex.getMessage());
  }

  // ── filter_type=efficient rejects script subtrees ───────────────────

  @Test
  void pushDownFilter_efficient_rejectsScriptSubtree() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder("{\"knn\":{\"filter\":\"embedded\"}}");
    var builder =
        new VectorSearchQueryBuilder(
            requestBuilder,
            knnQuery,
            Map.of("k", "5"),
            FilterType.EFFICIENT,
            true,
            rebuildWithFilter);

    // price + 1 > 100 lowers to a ScriptQueryBuilder; embedding it under knn.filter would
    // trigger the AOSS rejection this PR guards against.
    var condition =
        DSL.greater(
            DSL.add(new ReferenceExpression("price", ExprCoreType.INTEGER), DSL.literal(1)),
            DSL.literal(100));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> builder.pushDownFilter(filter));
    assertTrue(
        ex.getMessage().contains("vectorSearch WHERE pre-filtering does not support"),
        "Expected script rejection message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("script queries"),
        "Expected script queries guidance in message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("filter_type=post"),
        "Expected filter_type=post fallback guidance, got: " + ex.getMessage());
  }

  @Test
  void pushDownFilter_post_allowsScriptSubtree() {
    // POST puts WHERE in an outer bool.filter, not under knn.filter, so scripts are fine.
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder = new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("k", "5"));

    var condition =
        DSL.greater(
            DSL.add(new ReferenceExpression("price", ExprCoreType.INTEGER), DSL.literal(1)),
            DSL.literal(100));
    var dummyChild = new LogicalValues(Collections.emptyList());
    var filter = new LogicalFilter(dummyChild, condition);

    assertTrue(builder.pushDownFilter(filter), "POST mode must still accept script predicates");
  }

  @Test
  void buildSucceedsRadialWithSortEmbeddedLimit() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("{\"knn\":{}}");
    var builder =
        new VectorSearchQueryBuilder(requestBuilder, knnQuery, Map.of("max_distance", "10.0"));

    var dummyChild = new LogicalValues(Collections.emptyList());
    // LogicalSort with count=50 simulates PPL sort-with-limit path
    var sort =
        new org.opensearch.sql.planner.logical.LogicalSort(
            dummyChild,
            50,
            List.of(
                org.apache.commons.lang3.tuple.ImmutablePair.of(
                    org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC,
                    new ReferenceExpression("_score", ExprCoreType.FLOAT))));

    builder.pushDownSort(sort);

    // build() should not reject — limitPushed must be true via pushDownSort's count path
    OpenSearchRequestBuilder result = builder.build();
    assertNotNull(result);
  }

  // ── filter_type=efficient allow-list validator ──────────────────────

  @Test
  void validateEfficientFilterSafe_rejectsNestedQuery() {
    // FilterQueryBuilder emits NestedQueryBuilder for SQL nested(field, pred); nested vector
    // semantics are outside the P0 preview so rejection must be targeted, not generic.
    QueryBuilder nested =
        QueryBuilders.nestedQuery(
            "parent", QueryBuilders.termQuery("parent.f", "v"), ScoreMode.None);

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchQueryBuilder.validateEfficientFilterSafe(nested));
    assertTrue(
        ex.getMessage().contains("vectorSearch WHERE pre-filtering does not support nested"),
        "Expected targeted nested rejection, got: " + ex.getMessage());
  }

  @Test
  void validateEfficientFilterSafe_rejectsNestedBuriedInBool() {
    // AND-ing nested() with a term must still be caught; otherwise the guard is trivially bypassed.
    QueryBuilder tree =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("state", "CA"))
            .filter(
                QueryBuilders.nestedQuery(
                    "parent", QueryBuilders.termQuery("parent.f", "v"), ScoreMode.None));

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchQueryBuilder.validateEfficientFilterSafe(tree));
    assertTrue(ex.getMessage().contains("nested predicates"));
  }

  @Test
  void validateEfficientFilterSafe_acceptsBoolOfSafeLeaves() {
    QueryBuilder tree =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("category", "shoes"))
            .filter(QueryBuilders.rangeQuery("price").gte(80).lte(150));

    VectorSearchQueryBuilder.validateEfficientFilterSafe(tree);
  }

  @Test
  void validateEfficientFilterSafe_acceptsExistsLeaf() {
    // IS NOT NULL lowers to ExistsQueryBuilder; locks in allow-list coverage for that path.
    QueryBuilder exists = QueryBuilders.existsQuery("brand");

    VectorSearchQueryBuilder.validateEfficientFilterSafe(exists);
  }

  @Test
  void validateEfficientFilterSafe_rejectsUnknownWrapper() {
    // Unknown shapes must fail closed so future FilterQueryBuilder additions cannot silently
    // re-introduce the AOSS-rejection bug class this PR is guarding against.
    QueryBuilder unknown = new WrapperQueryBuilder("{\"term\":{\"f\":\"v\"}}");

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> VectorSearchQueryBuilder.validateEfficientFilterSafe(unknown));
    assertTrue(
        ex.getMessage().contains("unsupported filter query shape"),
        "Expected unknown-shape rejection, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("WrapperQueryBuilder"),
        "Expected class name in message, got: " + ex.getMessage());
  }

  private OpenSearchRequestBuilder createRequestBuilder() {
    return new OpenSearchRequestBuilder(
        mock(OpenSearchExprValueFactory.class), 10000, mock(Settings.class));
  }
}
