/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.QueryId;

/**
 * QueryExecution represent the execution entity of the Statement.
 */
@RequiredArgsConstructor
public abstract class QueryExecution {

  /**
   * Uniq query id.
   */
  @Getter
  private final QueryId queryId;

  /**
   * Start query execution.
   */
  public abstract void start();

  /**
   * Register response listener.
   * @param listener {@link ResponseListener}
   */
  public abstract void registerListener(ResponseListener<?> listener);
}
