/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalValues;

class VectorSearchIndexScanBuilderTest {

  @Test
  void pushDownAggregationIsRejected() {
    var requestBuilder =
        new OpenSearchRequestBuilder(
            mock(OpenSearchExprValueFactory.class), 10000, mock(Settings.class));
    var queryBuilder =
        new VectorSearchQueryBuilder(
            requestBuilder, new WrapperQueryBuilder("{\"knn\":{}}"), java.util.Map.of("k", "5"));
    var scanBuilder =
        new VectorSearchIndexScanBuilder(queryBuilder, rb -> mock(OpenSearchIndexScan.class));

    var agg =
        new LogicalAggregation(
            new LogicalValues(Collections.emptyList()),
            Collections.emptyList(),
            Collections.emptyList(),
            false);

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class, () -> scanBuilder.pushDownAggregation(agg));
    assertTrue(
        ex.getMessage().contains("Aggregations are not supported"),
        "Error should state aggregations are not supported; actual: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("vectorSearch"),
        "Error should mention vectorSearch; actual: " + ex.getMessage());
  }
}
