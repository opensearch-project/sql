/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstExpressionBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;

/**
 * SQL service.
 */
@RequiredArgsConstructor
public class SQLService {

  private final SQLSyntaxParser parser;

  private final QueryManager queryManager;

  private final QueryPlanFactory queryExecutionFactory;

  /**
   * Given {@link SQLQueryRequest}, execute it. Using listener to listen result.
   *
   * @param request {@link SQLQueryRequest}
   * @param listener callback listener
   */
  public void execute(SQLQueryRequest request, ResponseListener<QueryResponse> listener) {
    try {
      queryManager.submit(plan(request, Optional.of(listener), Optional.empty()));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Given {@link SQLQueryRequest}, explain it. Using listener to listen result.
   *
   * @param request {@link SQLQueryRequest}
   * @param listener callback listener
   */
  public void explain(SQLQueryRequest request, ResponseListener<ExplainResponse> listener) {
    try {
      queryManager.submit(plan(request, Optional.empty(), Optional.of(listener)));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private AbstractPlan plan(
      SQLQueryRequest request,
      Optional<ResponseListener<QueryResponse>> queryListener,
      Optional<ResponseListener<ExplainResponse>> explainListener) {
    boolean isExplainRequest = request.isExplainRequest();
    if (request.getCursor().isPresent()) {
      // Handle v2 cursor here -- legacy cursor was handled earlier.
      if (isExplainRequest) {
        throw new UnsupportedOperationException("Explain of a paged query continuation "
          + "is not supported. Use `explain` for the initial query request.");
      }
      if (request.isCursorCloseRequest()) {
        return queryExecutionFactory.createCloseCursor(request.getCursor().get(),
            queryListener.orElse(null));
      }
      return queryExecutionFactory.create(request.getCursor().get(),
        isExplainRequest, queryListener.orElse(null), explainListener.orElse(null));
    } else {
      // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
      ParseTree cst = parser.parse(request.getQuery());
      Statement statement =
          cst.accept(
              new AstStatementBuilder(
                  new AstBuilder(new AstExpressionBuilder(), request.getQuery()),
                  AstStatementBuilder.StatementBuilderContext.builder()
                      .isExplain(isExplainRequest)
                      .fetchSize(request.getFetchSize())
                      .build()));

      return queryExecutionFactory.create(
          statement, queryListener, explainListener);
    }
  }
}
