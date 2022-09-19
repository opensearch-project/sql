/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryExecution;
import org.opensearch.sql.executor.execution.QueryExecutionFactory;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;

/**
 * SQL service.
 */
@RequiredArgsConstructor
public class SQLService {

  private final SQLSyntaxParser parser;

  private final QueryManager queryManager;

  private final QueryExecutionFactory queryExecutionFactory;

  /**
   * Given {@link SQLQueryRequest}, execute it. Using listener to listen result.
   *
   * @param request {@link SQLQueryRequest}
   * @param listener callback listener
   */
  public void execute(SQLQueryRequest request, ResponseListener<QueryResponse> listener) {
    try {
      queryManager.submitQuery(plan(request), listener);
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
      queryManager.submitQuery(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private QueryExecution plan(SQLQueryRequest request) {
    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.parse(request.getQuery());
    Statement statement =
        cst.accept(
            new AstStatementBuilder(
                new AstBuilder(request.getQuery()),
                AstStatementBuilder.StatementBuilderContext.builder()
                    .isExplain(request.isExplainRequest())
                    .build()));

    return queryExecutionFactory.create(statement);
  }
}
