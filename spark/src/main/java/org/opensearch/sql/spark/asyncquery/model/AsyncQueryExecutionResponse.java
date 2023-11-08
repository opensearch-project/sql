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
}
