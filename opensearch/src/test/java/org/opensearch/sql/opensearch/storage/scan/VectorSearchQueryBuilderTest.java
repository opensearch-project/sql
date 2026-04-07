/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;

class VectorSearchQueryBuilderTest {

  @Test
  void knnQuerySetAsScoringQuery() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("eyJrbm4iOnt9fQ==");

    new VectorSearchQueryBuilder(requestBuilder, knnQuery);

    QueryBuilder query = requestBuilder.getSourceBuilder().query();
    assertTrue(
        query instanceof WrapperQueryBuilder,
        "knn query should be set directly as top-level query (scoring context)");
  }

  @Test
  void knnQueryNotWrappedInFilterWhenNoWhere() {
    var requestBuilder = createRequestBuilder();
    var knnQuery = new WrapperQueryBuilder("eyJrbm4iOnt9fQ==");

    new VectorSearchQueryBuilder(requestBuilder, knnQuery);

    QueryBuilder query = requestBuilder.getSourceBuilder().query();
    assertTrue(
        query instanceof WrapperQueryBuilder,
        "Without WHERE clause, knn query should NOT be wrapped in bool.filter");
  }

  private OpenSearchRequestBuilder createRequestBuilder() {
    return new OpenSearchRequestBuilder(
        mock(OpenSearchExprValueFactory.class), 10000, mock(Settings.class));
  }
}
