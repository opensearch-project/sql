/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryExecution;
import org.opensearch.sql.executor.execution.QueryExecutionFactory;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;
import org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizer;

/**
 * PPLService.
 */
@RequiredArgsConstructor
public class PPLService {
  private final PPLSyntaxParser parser;

  private final QueryManager queryManager;

  private final QueryExecutionFactory queryExecutionFactory;

  private final PPLQueryDataAnonymizer anonymizer = new PPLQueryDataAnonymizer();

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Execute the {@link PPLQueryRequest}, using {@link ResponseListener} to get response.
   *
   * @param request  {@link PPLQueryRequest}
   * @param listener {@link ResponseListener}
   */
  public void execute(PPLQueryRequest request, ResponseListener<QueryResponse> listener) {
    try {
      queryManager.submitQuery(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link PPLQueryRequest} using {@link ResponseListener} to
   * get and format explain response.
   *
   * @param request  {@link PPLQueryRequest}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(PPLQueryRequest request, ResponseListener<ExplainResponse> listener) {
    try {
      queryManager.submitQuery(plan(request), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private QueryExecution plan(PPLQueryRequest request) {
    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.parse(request.getRequest());
    Statement statement =
        cst.accept(
            new AstStatementBuilder(
                new AstBuilder(new AstExpressionBuilder(), request.getRequest()),
                AstStatementBuilder.StatementBuilderContext.builder()
                    .isExplain(request.isExplainRequest())
                    .build()));

    LOG.info("[{}] Incoming request {}", QueryContext.getRequestId(),
        anonymizer.anonymizeStatement(statement));

    return queryExecutionFactory.create(statement);
  }
}
