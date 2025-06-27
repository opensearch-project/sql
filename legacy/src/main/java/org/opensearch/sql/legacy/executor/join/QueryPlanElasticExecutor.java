/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.join;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    TimeValue customKeepAlive = extractJoinTimeoutFromSqlRequest();
    if (customKeepAlive != null) {
      LOG.info(
          "✅ QueryPlanElasticExecutor: Passing custom timeout to QueryPlanner: {} seconds",
          customKeepAlive.getSeconds());
      this.queryPlanner = request.plan(customKeepAlive);
    } else {
      LOG.info("⚠️ QueryPlanElasticExecutor: No custom timeout, using default QueryPlanner");
      this.queryPlanner = request.plan();
    }
  }

  @Override
  public void run() throws IOException, SqlParseException {
    try {
      long timeBefore = System.currentTimeMillis();

      LOG.info(
          "🔍 QueryPlanElasticExecutor: Starting execution, checking for JOIN_TIME_OUT hints...");

      // ✅ Extract JOIN_TIME_OUT hint from the original SQL request
      createPointInTimeWithCustomTimeout();
      results = innerRun();
      long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
      this.metaResults.setTookImMilli(joinTimeInMilli);
    } catch (Exception e) {
      LOG.error("Failed during QueryPlan join query run.", e);
      throw new IllegalStateException("Error occurred during QueryPlan join query run", e);
    } finally {
      cleanupPointInTime();
    }
  }

  /** Create Point-in-Time with custom timeout if JOIN_TIME_OUT hint is present */
  private void createPointInTimeWithCustomTimeout() {
    TimeValue customKeepAlive = extractJoinTimeoutFromSqlRequest();

    if (customKeepAlive != null) {
      LOG.info(
          "✅ QueryPlanElasticExecutor: Using custom PIT keepalive from JOIN_TIME_OUT hint: {}"
              + " seconds ({}ms)",
          customKeepAlive.getSeconds(),
          customKeepAlive.getMillis());
      pit = new PointInTimeHandlerImpl(client, indices, customKeepAlive);
    } else {
      LOG.info(
          "⚠️ QueryPlanElasticExecutor: No JOIN_TIME_OUT hint found, using default PIT keepalive");
      pit = new PointInTimeHandlerImpl(client, indices);
    }

    pit.create();
  }

  /** Clean up Point-in-Time resources */
  private void cleanupPointInTime() {
    if (pit != null) {
      try {
        pit.delete();
        LOG.debug("Successfully deleted PIT: {}", pit.getPitId());
      } catch (RuntimeException e) {
        Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
        LOG.warn("Error deleting point in time {}: {}", pit.getPitId(), e.getMessage(), e);
      }
    }
  }

  /** Extract JOIN_TIME_OUT hint directly from the original SQL request */
  private TimeValue extractJoinTimeoutFromSqlRequest() {
    try {
      LOG.info("🔍 Extracting hints from original SQL request...");

      // Access the private 'request' field from HashJoinQueryPlanRequestBuilder
      java.lang.reflect.Field requestField =
          planRequestBuilder.getClass().getDeclaredField("request");
      requestField.setAccessible(true);
      SqlRequest sqlRequest = (SqlRequest) requestField.get(planRequestBuilder);

      if (sqlRequest != null) {
        String originalSql = sqlRequest.getSql();
        // Parse JOIN_TIME_OUT hint from the SQL string
        TimeValue timeout = parseJoinTimeoutFromSql(originalSql);
        if (timeout != null) {
          LOG.info(
              "✅ Successfully extracted JOIN_TIME_OUT from SQL: {} seconds", timeout.getSeconds());
          return timeout;
        } else {
          LOG.info("⚠️ No JOIN_TIME_OUT hint found in SQL string");
        }
      } else {
        LOG.warn("⚠️ SqlRequest is null");
      }

    } catch (Exception e) {
      LOG.error("⚠️ Error accessing original SQL request", e);
      throw new RuntimeException("Unexpected error accessing SQL request", e);
    }

    return null;
  }

  /** Parse JOIN_TIME_OUT(number) hint from SQL string using regex */
  private TimeValue parseJoinTimeoutFromSql(String sql) {
    if (sql == null) {
      return null;
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
        return TimeValue.timeValueSeconds(timeoutSeconds);
      }

    } catch (Exception e) {
      LOG.error("Error parsing JOIN_TIME_OUT from SQL: {}", sql, e);
      throw new RuntimeException("Failed to parse JOIN_TIME_OUT from SQL: " + sql, e);
    }

    return null;
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
