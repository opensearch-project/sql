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
 * Scan builder for vector search relations. Rejects aggregation pushdown as a SQL preview
 * constraint: aggregations over vectorSearch() relations are not supported in the current preview.
 * Native OpenSearch k-NN does support aggregations alongside similarity search; this restriction is
 * specific to the SQL surface and may be lifted in a later release.
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
        "Aggregations are not supported on vectorSearch() relations in the SQL preview.");
  }
}
