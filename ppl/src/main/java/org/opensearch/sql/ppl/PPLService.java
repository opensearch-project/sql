/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.executor.execution.QueryPlanFactory.NO_CONSUMER_RESPONSE_LISTENER;

import lombok.extern.log4j.Log4j2;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Statement;
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
                    .format(request.getFormat())
                    .build()));

    log.info(
        "[{}] Incoming request {}",
        QueryContext.getRequestId(),
        anonymizer.anonymizeStatement(statement));

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  }

  /**
   * Public version of the plan method for testing purposes. This allows passing parameters to test
   * the execution plan.
   *
   * @param request {@link PPLQueryRequest}
   * @param queryListener {@link ResponseListener} for query response
   * @param explainListener {@link ResponseListener} for explain response
   * @return {@link AbstractPlan} execution plan
   */
  public AbstractPlan createPlan(
      PPLQueryRequest request,
      ResponseListener<QueryResponse> queryListener,
      ResponseListener<ExplainResponse> explainListener) {
    return plan(request, queryListener, explainListener);
  }

  /**
   * Execute the {@link PPLQueryRequest} with custom formatter parameters. This allows passing
   * additional parameters for testing purposes.
   *
   * @param request {@link PPLQueryRequest}
   * @param queryListener {@link ResponseListener}
   * @param explainListener {@link ResponseListener} for explain command
   * @param timechartLimit Maximum number of distinct values to display in timechart
   * @param timechartUseOther Whether to use OTHER category for values beyond the limit
   */
  public void executeWithFormatterParams(
      PPLQueryRequest request,
      ResponseListener<QueryResponse> queryListener,
      ResponseListener<ExplainResponse> explainListener,
      Integer timechartLimit,
      Boolean timechartUseOther) {
    try {
      // Create a new request with the custom formatter parameters
      PPLQueryRequest modifiedRequest =
          new PPLQueryRequest(
              request.getRequest(),
              request.getJsonContent(),
              request.getPath(),
              request.getFormat());

      // Set additional parameters using fluent interface
      modifiedRequest
          .sanitize(request.sanitize())
          .style(request.style())
          .timechartLimit(timechartLimit)
          .timechartUseOther(timechartUseOther);

      queryManager.submit(plan(modifiedRequest, queryListener, explainListener));
    } catch (Exception e) {
      queryListener.onFailure(e);
    }
  }

  /**
   * Create a TimechartResponseFormatter with custom parameters. This is a utility method for
   * testing purposes.
   *
   * @param style The JSON response style (PRETTY or COMPACT)
   * @param timechartLimit Maximum number of distinct values to display in timechart
   * @param timechartUseOther Whether to use OTHER category for values beyond the limit
   * @param isCountAggregation Whether the aggregation function is count()
   * @return A configured TimechartResponseFormatter
   */
  public static org.opensearch.sql.protocol.response.format.TimechartResponseFormatter
      createTimechartFormatter(
          org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style style,
          Integer timechartLimit,
          Boolean timechartUseOther,
          boolean isCountAggregation) {
    return new org.opensearch.sql.protocol.response.format.TimechartResponseFormatter(
            style, timechartLimit, timechartUseOther)
        .withCountAggregation(isCountAggregation);
  }

  /**
   * Parse a PPL query and return the statement. This is a utility method for testing purposes.
   *
   * @param pplQuery The PPL query string
   * @return The parsed statement
   */
  public Statement parseQuery(String pplQuery) {
    try {
      // Parse query and convert parse tree (CST) to abstract syntax tree (AST)
      ParseTree cst = parser.parse(pplQuery);
      return cst.accept(
          new AstStatementBuilder(
              new AstBuilder(pplQuery, settings),
              AstStatementBuilder.StatementBuilderContext.builder()
                  .isExplain(false)
                  .format("")
                  .build()));
    } catch (Exception e) {
      // Ignore parsing errors
      return null;
    }
  }

  /**
   * Extract timechart parameters from a query string. This is a utility method that uses regex to
   * extract limit and useOther parameters.
   *
   * @param query The PPL query string
   * @return A pair of [limit, useOther] parameters
   */
  public static TimechartParams extractTimechartParameters(String query) {
    if (query == null) {
      return new TimechartParams(null, true);
    }

    // Convert to lowercase for case-insensitive matching
    String lowerQuery = query.toLowerCase();

    // Extract limit parameter
    Integer limit = null;
    java.util.regex.Pattern limitPattern =
        java.util.regex.Pattern.compile("\\|\\s*timechart\\b.*?\\blimit\\s*=\\s*(\\d+)");
    java.util.regex.Matcher limitMatcher = limitPattern.matcher(lowerQuery);
    if (limitMatcher.find()) {
      try {
        limit = Integer.parseInt(limitMatcher.group(1));
      } catch (NumberFormatException e) {
        // Ignore parsing errors
      }
    }

    // Extract useOther parameter
    Boolean useOther = true; // Default is true
    java.util.regex.Pattern useOtherPattern =
        java.util.regex.Pattern.compile(
            "\\|\\s*timechart\\b.*?\\buseother\\s*=\\s*(true|t|false|f)\\b");
    java.util.regex.Matcher useOtherMatcher = useOtherPattern.matcher(lowerQuery);
    if (useOtherMatcher.find()) {
      String value = useOtherMatcher.group(1);
      useOther = "true".equals(value) || "t".equals(value);
    }

    return new TimechartParams(limit, useOther);
  }

  /** Simple class to hold timechart parameters. */
  public static class TimechartParams {
    private final Integer limit;
    private final Boolean useOther;

    public TimechartParams(Integer limit, Boolean useOther) {
      this.limit = limit;
      this.useOther = useOther;
    }

    public Integer getLimit() {
      return limit;
    }

    public Boolean getUseOther() {
      return useOther;
    }
  }
}
