/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.executor.execution.QueryPlanFactory.NO_CONSUMER_RESPONSE_LISTENER;

import lombok.extern.log4j.Log4j2;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Collect;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;
import org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizer;

/** PPLService. */
@Log4j2
public class PPLService {
  private final PPLSyntaxParser parser;

  private final QueryManager queryManager;

  private final QueryPlanFactory queryExecutionFactory;

  private final Settings settings;

  private final QueryType PPL_QUERY = QueryType.PPL;

  private final PPLQueryDataAnonymizer anonymizer;

  public PPLService(
      PPLSyntaxParser parser,
      QueryManager queryManager,
      QueryPlanFactory queryExecutionFactory,
      Settings settings) {
    this.parser = parser;
    this.queryManager = queryManager;
    this.queryExecutionFactory = queryExecutionFactory;
    this.settings = settings;
    this.anonymizer = new PPLQueryDataAnonymizer(settings);
  }

  /**
   * Execute the {@link PPLQueryRequest}, using {@link ResponseListener} to get response.
   *
   * @param request {@link PPLQueryRequest}
   * @param queryListener {@link ResponseListener}
   * @param explainListener {@link ResponseListener} for explain command
   */
  public void execute(
      PPLQueryRequest request,
      ResponseListener<QueryResponse> queryListener,
      ResponseListener<ExplainResponse> explainListener) {
    try {
      queryManager.submit(plan(request, queryListener, explainListener));
    } catch (Exception e) {
      queryListener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link PPLQueryRequest} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param request {@link PPLQueryRequest}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(PPLQueryRequest request, ResponseListener<ExplainResponse> listener) {
    try {
      queryManager.submit(plan(request, NO_CONSUMER_RESPONSE_LISTENER, listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Execute an already-parsed {@link Statement}, bypassing the parser. Used by the async collect
   * route to run the preview plan it built from the parsed AST, so a routed collect query is parsed
   * exactly once.
   */
  public void executeStatement(
      Statement statement,
      ResponseListener<QueryResponse> queryListener,
      ResponseListener<ExplainResponse> explainListener) {
    try {
      queryManager.submit(queryExecutionFactory.create(statement, queryListener, explainListener));
    } catch (Exception e) {
      queryListener.onFailure(e);
    }
  }

  /**
   * If the query's terminal (outermost) command is a non-testmode {@code collect}, returns the
   * destination index name together with a ready preview statement built from the parsed AST (the
   * collect stripped, its upstream capped at QUERY_SIZE_LIMIT); otherwise empty. The transport
   * layer routes such a query to the async materialization action and runs the returned preview via
   * {@link #executeStatement}, so the query is parsed once and the preview neither re-parses nor
   * text strips the collect. Parse failures return empty so the normal execution path reports the
   * error. testmode collect is a dry run (no write) and stays on the synchronous path.
   */
  public java.util.Optional<CollectRoute> routeTerminalCollect(PPLQueryRequest request) {
    try {
      ParseTree cst = parser.parse(request.getRequest());
      Statement statement =
          cst.accept(
              new AstStatementBuilder(
                  new AstBuilder(request.getRequest(), settings),
                  AstStatementBuilder.StatementBuilderContext.builder()
                      .isExplain(request.isExplainRequest())
                      .fetchSize(request.getFetchSize())
                      .highlightConfig(request.getHighlightConfig())
                      .format(request.getFormat())
                      .explainMode(request.getExplainMode())
                      .build()));
      if (statement instanceof Query query
          && query.getPlan() instanceof Collect collect
          && !collect.isTestmode()) {
        int cap = settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT);
        UnresolvedPlan upstream = collect.getChild().get(0);
        Query preview =
            new Query(new Head(upstream, cap, 0), query.getFetchSize(), query.getQueryType());
        return java.util.Optional.of(new CollectRoute(collect.getIndexName(), preview));
      }
    } catch (Exception ignored) {
      // not routable as async collect; the normal execution path handles or reports it
    }
    return java.util.Optional.empty();
  }

  /** Destination index and preview statement for a terminal collect routed to the async action. */
  public static final class CollectRoute {
    private final String destIndex;
    private final Statement previewStatement;

    public CollectRoute(String destIndex, Statement previewStatement) {
      this.destIndex = destIndex;
      this.previewStatement = previewStatement;
    }

    public String getDestIndex() {
      return destIndex;
    }

    public Statement getPreviewStatement() {
      return previewStatement;
    }
  }

  private AbstractPlan plan(
      PPLQueryRequest request,
      ResponseListener<QueryResponse> queryListener,
      ResponseListener<ExplainResponse> explainListener) {
    // 1.Parse query and convert parse tree (CST) to abstract syntax tree (AST)
    ParseTree cst = parser.parse(request.getRequest());
    Statement statement =
        cst.accept(
            new AstStatementBuilder(
                new AstBuilder(request.getRequest(), settings),
                AstStatementBuilder.StatementBuilderContext.builder()
                    .isExplain(request.isExplainRequest())
                    .fetchSize(request.getFetchSize())
                    .highlightConfig(request.getHighlightConfig())
                    .format(request.getFormat())
                    .explainMode(request.getExplainMode())
                    .build()));

    log.info(
        "[{}] Incoming request {}",
        QueryContext.getRequestId(),
        anonymizer.anonymizeStatement(statement));

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }
}
