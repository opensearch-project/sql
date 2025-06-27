/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.join;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.pit.PointInTimeHandlerImpl;
import org.opensearch.sql.legacy.query.planner.HashJoinQueryPlanRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.QueryPlanner;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.transport.client.Client;

/**
 * Executor for generic QueryPlanner execution. This executor is just acting as adaptor to integrate
 * with existing framework. In future, QueryPlanner should be executed by itself and leave the
 * response sent back or other post-processing logic to ElasticDefaultRestExecutor.
 */
class QueryPlanElasticExecutor extends ElasticJoinExecutor {

  private final QueryPlanner queryPlanner;
  private final HashJoinQueryPlanRequestBuilder planRequestBuilder;

  QueryPlanElasticExecutor(Client client, HashJoinQueryPlanRequestBuilder request) {
    super(client, request);
    this.planRequestBuilder = request;

    // Create QueryPlanner with custom timeout if available
    Optional<TimeValue> customKeepAlive = extractJoinTimeoutFromSqlRequest();
    if (customKeepAlive.isPresent()) {
      TimeValue keepAlive = customKeepAlive.get();
      LOG.info(
          "✅ QueryPlanElasticExecutor: Passing custom timeout to QueryPlanner: {} seconds",
          keepAlive.getSeconds());
      this.queryPlanner = request.plan(keepAlive);
    } else {
      LOG.info("⚠️ QueryPlanElasticExecutor: No custom timeout, using default QueryPlanner");
      this.queryPlanner = request.plan();
    }
  }

  @Override
  public void run() throws IOException, SqlParseException {
    try {
      long timeBefore = System.currentTimeMillis();

      LOG.debug(
          "🔍 QueryPlanElasticExecutor: Starting execution, checking for JOIN_TIME_OUT hints...");

      // Create PIT with appropriate timeout
      createPointInTimeWithCustomTimeout();
      results = innerRun();
      long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
      this.metaResults.setTookImMilli(joinTimeInMilli);

    } catch (RuntimeException e) {
      LOG.error("Runtime error during QueryPlan join query execution", e);
      throw new IllegalStateException("Error occurred during QueryPlan join query run", e);
    } catch (Exception e) {
      LOG.error("Unexpected error during QueryPlan join query execution", e);
      throw new IllegalStateException(
          "Unexpected error occurred during QueryPlan join query run", e);
    } finally {
      cleanupPointInTime();
    }
  }

  /** Create Point-in-Time with custom timeout if JOIN_TIME_OUT hint is present */
  private void createPointInTimeWithCustomTimeout() {
    Optional<TimeValue> customKeepAlive = extractJoinTimeoutFromSqlRequest();

    if (customKeepAlive.isPresent()) {
      TimeValue keepAlive = customKeepAlive.get();
      LOG.info(
          "✅ QueryPlanElasticExecutor: Using custom PIT keepalive from JOIN_TIME_OUT hint: {}"
              + " seconds ({}ms)",
          keepAlive.getSeconds(),
          keepAlive.getMillis());
      pit = new PointInTimeHandlerImpl(client, indices, keepAlive);
    } else {
      LOG.info(
          "⚠️ QueryPlanElasticExecutor: No JOIN_TIME_OUT hint found, using default PIT keepalive");
      pit = new PointInTimeHandlerImpl(client, indices);
    }

    pit.create();
  }

  /** Clean up Point-in-Time resources safely */
  private void cleanupPointInTime() {
    if (pit != null) {
      try {
        pit.delete();
        LOG.debug("Successfully deleted PIT");
      } catch (RuntimeException e) {
        Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
        LOG.error("Error deleting point in time: {}", e.getMessage(), e);
      }
    }
  }

  /**
   * Extract JOIN_TIME_OUT hint directly from the original SQL request
   *
   * @return Optional containing TimeValue if JOIN_TIME_OUT hint found, empty otherwise
   */
  private Optional<TimeValue> extractJoinTimeoutFromSqlRequest() {
    try {
      LOG.debug("Extracting hints from original SQL request...");

      // Access the private 'request' field from HashJoinQueryPlanRequestBuilder
      java.lang.reflect.Field requestField =
          planRequestBuilder.getClass().getDeclaredField("request");
      requestField.setAccessible(true);
      SqlRequest sqlRequest = (SqlRequest) requestField.get(planRequestBuilder);

      if (sqlRequest != null) {
        String originalSql = sqlRequest.getSql();
        LOG.debug("Original SQL: {}", originalSql);

        // Parse JOIN_TIME_OUT hint from the SQL string
        Optional<TimeValue> timeout = parseJoinTimeoutFromSql(originalSql);
        if (timeout.isPresent()) {
          LOG.info(
              "✅ Successfully extracted JOIN_TIME_OUT from SQL: {} seconds",
              timeout.get().getSeconds());
          return timeout;
        } else {
          LOG.debug("No JOIN_TIME_OUT hint found in SQL string");
        }
      } else {
        LOG.warn("SqlRequest is null");
      }

    } catch (NoSuchFieldException e) {
      LOG.warn(
          "Could not find 'request' field in HashJoinQueryPlanRequestBuilder - class structure may"
              + " have changed",
          e);
    } catch (IllegalAccessException e) {
      LOG.warn(
          "Could not access 'request' field in HashJoinQueryPlanRequestBuilder - security"
              + " restrictions",
          e);
    } catch (ClassCastException e) {
      LOG.warn("Request field is not of type SqlRequest - unexpected class structure", e);
    } catch (SecurityException e) {
      LOG.warn("Security manager prevented access to request field", e);
    }

    return Optional.empty();
  }

  /**
   * Parse JOIN_TIME_OUT(number) hint from SQL string using regex
   *
   * @param sql the SQL string to parse
   * @return Optional containing TimeValue if JOIN_TIME_OUT hint found, empty otherwise
   */
  private Optional<TimeValue> parseJoinTimeoutFromSql(String sql) {
    if (sql == null) {
      return Optional.empty();
    }

    try {
      // Regex pattern to match /*! JOIN_TIME_OUT(number) */
      Pattern pattern =
          Pattern.compile(
              "/\\*!\\s*JOIN_TIME_OUT\\s*\\(\\s*(\\d+)\\s*\\)\\s*\\*/", Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(sql);

      if (matcher.find()) {
        String timeoutStr = matcher.group(1);
        int timeoutSeconds = Integer.parseInt(timeoutStr);

        LOG.info("✅ Parsed JOIN_TIME_OUT hint: {} seconds", timeoutSeconds);
        return Optional.of(TimeValue.timeValueSeconds(timeoutSeconds));
      }

    } catch (NumberFormatException e) {
      LOG.warn("Invalid JOIN_TIME_OUT value in SQL - not a valid number: {}", sql, e);
    } catch (PatternSyntaxException e) {
      LOG.warn("Regex pattern error while parsing JOIN_TIME_OUT from SQL: {}", sql, e);
    } catch (IllegalStateException e) {
      LOG.warn("Regex matcher error while parsing JOIN_TIME_OUT from SQL: {}", sql, e);
    }

    return Optional.empty();
  }

  @Override
  protected List<SearchHit> innerRun() {
    List<SearchHit> result = queryPlanner.execute();
    populateMetaResult();
    return result;
  }

  private void populateMetaResult() {
    metaResults.addTotalNumOfShards(queryPlanner.getMetaResult().getTotalNumOfShards());
    metaResults.addSuccessfulShards(queryPlanner.getMetaResult().getSuccessfulShards());
    metaResults.addFailedShards(queryPlanner.getMetaResult().getFailedShards());
    metaResults.updateTimeOut(queryPlanner.getMetaResult().isTimedOut());
  }
}
