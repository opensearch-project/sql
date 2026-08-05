/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.executor.execution.QueryPlanFactory.NO_CONSUMER_RESPONSE_LISTENER;

import java.util.function.Consumer;
import lombok.extern.log4j.Log4j2;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;
import org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizer;

/** PPLService. */
@Log4j2
public class PPLService {

  /** Callers that don't care about the anonymized query pass this. */
  public static final Consumer<String> NO_ANONYMIZED_QUERY_SINK = s -> {};

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
    execute(request, queryListener, explainListener, NO_ANONYMIZED_QUERY_SINK);
  }

  /** Variant that hands the anonymized query text to {@code anonymizedQuerySink}. */
  public void execute(
      PPLQueryRequest request,
      ResponseListener<QueryResponse> queryListener,
      ResponseListener<ExplainResponse> explainListener,
      Consumer<String> anonymizedQuerySink) {
    try {
      queryManager.submit(plan(request, queryListener, explainListener, anonymizedQuerySink));
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
    explain(request, listener, NO_ANONYMIZED_QUERY_SINK);
  }

  /** Variant that hands the anonymized query text to {@code anonymizedQuerySink}. */
  public void explain(
      PPLQueryRequest request,
      ResponseListener<ExplainResponse> listener,
      Consumer<String> anonymizedQuerySink) {
    try {
      queryManager.submit(
          plan(request, NO_CONSUMER_RESPONSE_LISTENER, listener, anonymizedQuerySink));
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
    analyze(request, listener, NO_ANONYMIZED_QUERY_SINK);
  }

  /** Variant that hands the anonymized query text to {@code anonymizedQuerySink}. */
  public void analyze(
      PPLQueryRequest request,
      ResponseListener<AnalyzeResponse> listener,
      Consumer<String> anonymizedQuerySink) {
    try {
      String queryText = request.getRequest();
      ParseTree cst;
      Statement statement;
      String anonymized;
      // Transport-thread work — parse, AST build, anonymize. Trace-only: QueryProfiling isn't
      // active yet on this thread. Cold-start ANTLR grammar init dominates this region.
      try (ProfileScope preparePhase = ProfileScope.openTraceOnly("prepare")) {
        try {
          cst = parser.parse(queryText);
          statement =
              cst.accept(
                  new AstStatementBuilder(
                      new AstBuilder(queryText, settings),
                      AstStatementBuilder.StatementBuilderContext.builder()
                          .isExplain(false)
                          .fetchSize(request.getFetchSize())
                          .highlightConfig(request.getHighlightConfig())
                          .format(
                              request.getFormat() != null && !request.getFormat().isEmpty()
                                  ? org.opensearch.sql.protocol.response.format.Format.ofExplain(
                                          request.getFormat())
                                      .orElse(null)
                                  : null)
                          .build()));
          anonymized = anonymizer.anonymizeStatement(statement);
        } catch (Exception e) {
          preparePhase.setError(e);
          throw e;
        }
      }
      log.info("[{}] Incoming request {}", QueryContext.getRequestId(), anonymized);
      anonymizedQuerySink.accept(anonymized);

      UnresolvedPlan unresolvedPlan = ((Query) statement).getPlan();
      queryManager.submit(
          queryExecutionFactory.createAnalyzePlan(unresolvedPlan, PPL_QUERY, listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private AbstractPlan plan(
      PPLQueryRequest request,
      ResponseListener<QueryResponse> queryListener,
      ResponseListener<ExplainResponse> explainListener,
      Consumer<String> anonymizedQuerySink) {
    Statement statement;
    String anonymized;
    // Transport-thread work — parse, AST build, anonymize. Trace-only: QueryProfiling isn't
    // active yet on this thread. Cold-start ANTLR grammar init dominates this region.
    try (ProfileScope preparePhase = ProfileScope.openTraceOnly("prepare")) {
      try {
        // 1. Parse query and convert parse tree (CST) to abstract syntax tree (AST)
        ParseTree cst = parser.parse(request.getRequest());
        boolean includeMetadata = request.getIncludeMetadata();
        statement =
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
                        .includeMetadata(includeMetadata)
                        .build()));
        anonymized = anonymizer.anonymizeStatement(statement);
      } catch (RuntimeException e) {
        preparePhase.setError(e);
        throw e;
      }
    }
    log.info("[{}] Incoming request {}", QueryContext.getRequestId(), anonymized);
    anonymizedQuerySink.accept(anonymized);

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }
}
