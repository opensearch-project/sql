/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;

/**
 * QueryExecution Factory.
 */
@RequiredArgsConstructor
public class QueryPlanFactory
    extends AbstractNodeVisitor<
        AbstractPlan,
        Pair<
            Optional<ResponseListener<ExecutionEngine.QueryResponse>>,
            Optional<ResponseListener<ExecutionEngine.ExplainResponse>>>> {

  /**
   * Query Service.
   */
  private final QueryService queryService;

  /**
   * NO_CONSUMER_RESPONSE_LISTENER should never been called. It is only used as constructor
   * parameter of {@link QueryPlan}.
   */
  @VisibleForTesting
  protected static final ResponseListener<ExecutionEngine.QueryResponse>
      NO_CONSUMER_RESPONSE_LISTENER =
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.QueryResponse response) {
              throw new IllegalStateException(
                  "[BUG] query response should not sent to unexpected channel");
            }

            @Override
            public void onFailure(Exception e) {
              throw new IllegalStateException(
                  "[BUG] exception response should not sent to unexpected channel");
            }
          };

  /**
   * Create QueryExecution from Statement.
   */
  public AbstractPlan create(
      Statement statement,
      Optional<ResponseListener<ExecutionEngine.QueryResponse>> queryListener,
      Optional<ResponseListener<ExecutionEngine.ExplainResponse>> explainListener) {
    return statement.accept(this, Pair.of(queryListener, explainListener));
  }

  @Override
  public AbstractPlan visitQuery(
      Query node,
      Pair<
              Optional<ResponseListener<ExecutionEngine.QueryResponse>>,
              Optional<ResponseListener<ExecutionEngine.ExplainResponse>>>
          context) {
    Preconditions.checkArgument(
        context.getLeft().isPresent(), "[BUG] query listener must be not null");

    return new QueryPlan(QueryId.queryId(), node.getPlan(), queryService, context.getLeft().get());
  }

  @Override
  public AbstractPlan visitExplain(
      Explain node,
      Pair<
              Optional<ResponseListener<ExecutionEngine.QueryResponse>>,
              Optional<ResponseListener<ExecutionEngine.ExplainResponse>>>
          context) {
    Preconditions.checkArgument(
        context.getRight().isPresent(), "[BUG] explain listener must be not null");

    return new ExplainPlan(
        QueryId.queryId(),
        create(node.getStatement(), Optional.of(NO_CONSUMER_RESPONSE_LISTENER), Optional.empty()),
        context.getRight().get());
  }
}
