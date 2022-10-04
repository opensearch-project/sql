/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl;

import lombok.Setter;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;

/**
 * Data definition task interface.
 */
@Setter
public abstract class DataDefinitionTask {

  /**
   * Execute DDL statement.
   */
  public abstract void execute(QueryService queryService,
                               ResponseListener<ExecutionEngine.QueryResponse> listener);

}
