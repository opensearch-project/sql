/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import java.util.Optional;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;

public class DDLQueryExecution extends QueryExecution {

  /**
   * The query plan ast.
   */
  private final DataDefinitionTask dataDefinitionTask;

  /**
   * True if the QueryExecution is explain only.
   */
  private final boolean isExplain;

  /**
   * Query service.
   */
  private final QueryService queryService;

  /**
   * Response listener.
   */
  private Optional<ResponseListener<?>> listener = Optional.empty();

  /** constructor. */
  public DDLQueryExecution(
      QueryId queryId,
      DataDefinitionTask dataDefinitionTask,
      boolean isExplain,
      QueryService queryService) {
    super(queryId);
    this.dataDefinitionTask = dataDefinitionTask;
    this.isExplain = isExplain;
    this.queryService = queryService;
  }

  @Override
  public void start() {
    dataDefinitionTask.execute(
        queryService, (ResponseListener<ExecutionEngine.QueryResponse>) listener.get());
  }

  @Override
  public void registerListener(ResponseListener<?> listener) {
    this.listener = Optional.of(listener);
  }
}
