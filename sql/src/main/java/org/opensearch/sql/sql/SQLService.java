/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.analysis.JsonSupportVisitor;
import org.opensearch.sql.analysis.JsonSupportVisitorContext;
import org.opensearch.sql.ast.statement.Query;
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
    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.parse(request.getQuery());
    Statement statement =
        cst.accept(
            new AstStatementBuilder(
                new AstBuilder(request.getQuery()),
                AstStatementBuilder.StatementBuilderContext.builder()
                    .isExplain(request.isExplainRequest())
                    .build()));

    // There is no full support for JSON format yet for in memory operations, aliases, literals,
    // and casts. Aggregation has differences with legacy results.
    if (request.format().getFormatName().equals("json") && statement instanceof Query) {
      if (parser.parseHints(request.getQuery()).getChildCount() > 1) {
        throw new UnsupportedOperationException("Hints are not yet supported in the new engine.");
      }

      // Go through the tree and throw exceptions when unsupported
      JsonSupportVisitorContext jsonSupportVisitorContext = new JsonSupportVisitorContext();
      if (!((Query) statement).getPlan().accept(new JsonSupportVisitor(),
          jsonSupportVisitorContext)) {
        throw new UnsupportedOperationException(
            "The following features are not supported with JSON format: "
                .concat(jsonSupportVisitorContext.getUnsupportedNodes().stream()
                .collect(Collectors.joining(", "))));
      }
    }

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }
}
