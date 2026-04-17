/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.function.Function;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;

/**
 * Scan builder for vector search relations. Rejects aggregation pushdown because the semantics of
 * aggregations over knn-scored documents are ambiguous at the SQL surface (e.g., per-bucket _score
 * is undefined). Rejecting at push-down time keeps the MVP contract tight; support can be added
 * later without breaking existing users.
 */
public class VectorSearchIndexScanBuilder extends OpenSearchIndexScanBuilder {

  public VectorSearchIndexScanBuilder(
      PushDownQueryBuilder translator,
      Function<OpenSearchRequestBuilder, OpenSearchIndexScan> scanFactory) {
    super(translator, scanFactory);
  }

  @Override
  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    throw new ExpressionEvaluationException(
        "GROUP BY / aggregations are not supported on vectorSearch() relations. "
            + "Wrap the vectorSearch() call in a subquery and aggregate over the subquery result.");
  }
}
