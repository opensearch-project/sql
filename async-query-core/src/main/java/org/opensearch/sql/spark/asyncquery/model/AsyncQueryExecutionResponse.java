/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.model;

import java.util.List;
import lombok.Data;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

/** AsyncQueryExecutionResponse to store the response form spark job execution. */
@Data
public class AsyncQueryExecutionResponse {
  private final String status;
  private final ExecutionEngine.Schema schema;
  private final List<ExprValue> results;
  private final String error;
  private final String sessionId;

  /**
   * Pre-formatted JSON body for a statement-level {@code explain} result. Populated only when the
   * underlying job produced a {@link org.opensearch.sql.job.QueryResult.Explain}; {@code null} for
   * ordinary row-shaped responses. Transport layers that see a non-null value bypass the standard
   * schema-and-datarows renderer and return this string verbatim.
   */
  private final String explainJson;
}
