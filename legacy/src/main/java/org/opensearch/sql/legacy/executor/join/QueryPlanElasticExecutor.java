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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

  private static final Logger LOG = LogManager.getLogger();

  private final QueryPlanner queryPlanner;
  private final HashJoinQueryPlanRequestBuilder planRequestBuilder;

  QueryPlanElasticExecutor(Client client, HashJoinQueryPlanRequestBuilder request) {
    super(client, request);
    this.planRequestBuilder = request;

    Optional<TimeValue> customKeepAlive = extractJoinTimeoutFromSqlRequest();

    customKeepAlive.ifPresentOrElse(
        timeout -> {
          LOG.info(
              "QueryPlanElasticExecutor: Found custom timeout, storing in Config and Request: {}"
                  + " seconds",
              timeout.getSeconds());
          request.getConfig().setCustomPitKeepAlive(timeout);
          request.setCustomPitKeepAlive(timeout);
        },
        () -> LOG.info("QueryPlanElasticExecutor: No custom timeout found, using defaults"));

    this.queryPlanner = request.plan();
  }

  @Override
  public void run() throws IOException, SqlParseException {
    try {
      long timeBefore = System.currentTimeMillis();

      LOG.debug("QueryPlanElasticExecutor: Starting execution");

      pit = new PointInTimeHandlerImpl(client, indices, planRequestBuilder.getConfig());
      pit.create();
      results = innerRun();
      long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
      this.metaResults.setTookImMilli(joinTimeInMilli);
    } catch (Exception e) {
      LOG.error("Failed during QueryPlan join query run.", e);
      throw new IllegalStateException("Error occurred during QueryPlan join query run", e);
    } finally {
      try {
        if (pit != null) {
          pit.delete();
        }
      } catch (RuntimeException e) {
        Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
        LOG.debug("Error deleting point in time {}", pit);
      }
    }
  }

  /**
   * Extract JOIN_TIME_OUT hint directly from the original SQL request
   *
   * @return Optional containing timeout if found, otherwise empty
   */
  private Optional<TimeValue> extractJoinTimeoutFromSqlRequest() {
    try {
      LOG.debug("Extracting hints from original SQL request");

      // Access the private 'request' field from HashJoinQueryPlanRequestBuilder
      java.lang.reflect.Field requestField =
          planRequestBuilder.getClass().getDeclaredField("request");
      requestField.setAccessible(true);
      SqlRequest sqlRequest = (SqlRequest) requestField.get(planRequestBuilder);

      if (sqlRequest != null) {
        String originalSql = sqlRequest.getSql();
        return parseJoinTimeoutFromSql(originalSql);
      } else {
        LOG.debug("SqlRequest is null");
      }

    } catch (Exception e) {
      LOG.error("Error accessing original SQL request: {}", e.getMessage());
    }

    return Optional.empty();
  }

  /**
   * Parse JOIN_TIME_OUT(number) hint from SQL string using regex
   *
   * @param sql SQL string to parse
   * @return Optional containing TimeValue if hint found, otherwise empty
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

        LOG.debug("Parsed JOIN_TIME_OUT hint: {} seconds", timeoutSeconds);
        return Optional.of(TimeValue.timeValueSeconds(timeoutSeconds));
      }

    } catch (Exception e) {
      LOG.error("Error parsing JOIN_TIME_OUT from SQL: {}", e.getMessage());
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
