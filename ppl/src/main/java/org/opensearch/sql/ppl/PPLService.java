/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.executor.execution.QueryPlanFactory.NO_CONSUMER_RESPONSE_LISTENER;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.AnalyzeResponse.QuerySegment;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
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
   * Analyze the query: produces the AST node and logical plan RelNode.
   *
   * @param request {@link PPLQueryRequest}
   * @param listener {@link ResponseListener} for analyze response
   */
  public void analyze(PPLQueryRequest request, ResponseListener<AnalyzeResponse> listener) {
    try {
      String queryText = request.getRequest();
      ParseTree cst = parser.parse(queryText);
      Statement statement =
          cst.accept(
              new AstStatementBuilder(
                  new AstBuilder(queryText, settings),
                  AstStatementBuilder.StatementBuilderContext.builder()
                      .isExplain(false)
                      .fetchSize(request.getFetchSize())
                      .highlightConfig(request.getHighlightConfig())
                      .format(request.getFormat())
                      .build()));

      log.info(
          "[{}] Incoming request {}",
          QueryContext.getRequestId(),
          anonymizer.anonymizeStatement(statement));

      List<QuerySegment> querySegments = extractQuerySegments(cst, queryText);
      UnresolvedPlan unresolvedPlan = ((Query) statement).getPlan();
      queryManager.submit(
          queryExecutionFactory.createAnalyzePlan(
              queryText, querySegments, unresolvedPlan, PPL_QUERY, listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private List<QuerySegment> extractQuerySegments(ParseTree cst, String queryText) {
    List<QuerySegment> segments = new ArrayList<>();
    OpenSearchPPLParser.QueryStatementContext queryStmt = findQueryStatement(cst);
    if (queryStmt == null) {
      return segments;
    }

    // First segment: the search/source command (pplCommands)
    OpenSearchPPLParser.PplCommandsContext pplCommands = queryStmt.pplCommands();
    if (pplCommands != null) {
      segments.add(buildSegment(pplCommands, queryText));
    }

    // Remaining segments: each piped command
    for (OpenSearchPPLParser.CommandsContext cmd : queryStmt.commands()) {
      segments.add(buildSegment(cmd, queryText));
    }
    return segments;
  }

  private OpenSearchPPLParser.QueryStatementContext findQueryStatement(ParseTree tree) {
    if (tree instanceof OpenSearchPPLParser.QueryStatementContext ctx) {
      return ctx;
    }
    for (int i = 0; i < tree.getChildCount(); i++) {
      OpenSearchPPLParser.QueryStatementContext result = findQueryStatement(tree.getChild(i));
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  private QuerySegment buildSegment(ParserRuleContext ctx, String queryText) {
    int start = ctx.getStart().getStartIndex();
    int stop = ctx.getStop().getStopIndex();
    String source = queryText.substring(start, stop + 1);
    // For wrapper rules like CommandsContext, drill into the specific child command
    ParserRuleContext target = ctx;
    if (ctx.getChildCount() == 1 && ctx.getChild(0) instanceof ParserRuleContext child) {
      target = child;
    }
    String nodeType = target.getClass().getSimpleName().replace("Context", "");
    return QuerySegment.builder()
        .nodeType(nodeType)
        .source(source)
        .build();
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
                    .format(
                        request.getFormat() != null && !request.getFormat().isEmpty()
                            ? org.opensearch.sql.protocol.response.format.Format.ofExplain(
                                    request.getFormat())
                                .orElse(null)
                            : null)
                    .explainMode(request.getExplainMode())
                    .build()));

    log.info(
        "[{}] Incoming request {}",
        QueryContext.getRequestId(),
        anonymizer.anonymizeStatement(statement));

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }
}
