/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  private OpenSearchRequestBuilder createRequestBuilder() {
    return new OpenSearchRequestBuilder(
        mock(OpenSearchExprValueFactory.class), 10000, mock(Settings.class));
  }
}
