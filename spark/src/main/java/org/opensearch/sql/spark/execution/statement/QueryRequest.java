/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import lombok.Data;
import org.opensearch.sql.spark.rest.model.LangType;

@Data
public class QueryRequest {
  private final LangType langType;
  private final String query;
}
