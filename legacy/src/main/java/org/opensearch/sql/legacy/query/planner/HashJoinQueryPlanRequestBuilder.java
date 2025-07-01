/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.legacy.query.planner;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.legacy.query.join.HashJoinElasticRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.Config;
import org.opensearch.sql.legacy.query.planner.core.QueryParams;
import org.opensearch.sql.legacy.query.planner.core.QueryPlanner;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.transport.client.Client;

/**
 * QueryPlanner builder for Hash Join query. In the future, different queries could have its own
 * builders to generate QueryPlanner. QueryPlanner would run all stages in its pipeline no matter
 * how it is assembled.
 */
public class HashJoinQueryPlanRequestBuilder extends HashJoinElasticRequestBuilder {
  private static final Logger LOG = LogManager.getLogger();

  /** Client connection to OpenSearch cluster */
  private final Client client;

  /** Query request */
  private final SqlRequest request;

  /** Query planner configuration */
  private final Config config;

  public HashJoinQueryPlanRequestBuilder(Client client, SqlRequest request) {
    this.client = client;
    this.request = request;
    this.config = new Config();

    // Parse JOIN_TIME_OUT hint and configure immediately
    parseJoinTimeoutFromSql(request.getSql())
        .ifPresent(
            timeout -> {
              LOG.info(
                  "HashJoinQueryPlanRequestBuilder: Found JOIN_TIME_OUT hint: {} seconds,"
                      + " configuring PIT keepalive",
                  timeout.getSeconds());
              config.setCustomPitKeepAlive(timeout);
            });
  }

  @Override
  public String explain() {
    return plan().explain();
  }

  /**
   * Planning for the query and create planner for explain/execute later.
   *
   * @return query planner
   */
  public QueryPlanner plan() {
    config.configureLimit(
        getTotalLimit(), getFirstTable().getHintLimit(), getSecondTable().getHintLimit());
    config.configureTermsFilterOptimization(isUseTermFiltersOptimization());

    // Set the hint config on both table builders so they can access JOIN_TIME_OUT
    // Note: JOIN_TIME_OUT is a query-level hint that affects all PIT operations in the join
    if (config.hasCustomPitKeepAlive()) {
      LOG.debug("Setting hint config on both table builders for JOIN_TIME_OUT propagation");
      getFirstTable().setHintConfig(config);
      getSecondTable().setHintConfig(config);
    }

    return new QueryPlanner(
        client,
        config,
        new QueryParams(
            getFirstTable(), getSecondTable(), getJoinType(), getT1ToT2FieldsComparison()));
  }

  public Config getConfig() {
    return config;
  }

  /**
   * Parse JOIN_TIME_OUT(number) hint from SQL string using regex
   *
   * @param sql SQL string to parse
   * @return Optional containing TimeValue if hint found, otherwise empty
   */
  private Optional<TimeValue> parseJoinTimeoutFromSql(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      LOG.debug("SQL string is null or empty, no JOIN_TIME_OUT hint to parse");
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

        if (timeoutSeconds <= 0) {
          LOG.warn(
              "JOIN_TIME_OUT hint has invalid value: {} seconds. Must be positive integer.",
              timeoutSeconds);
          return Optional.empty();
        }

        LOG.debug("Successfully parsed JOIN_TIME_OUT hint: {} seconds", timeoutSeconds);
        return Optional.of(TimeValue.timeValueSeconds(timeoutSeconds));
      } else {
        LOG.debug(
            "No JOIN_TIME_OUT hint found in SQL: {}",
            sql.substring(0, Math.min(sql.length(), 100)) + "...");
      }

    } catch (NumberFormatException e) {
      LOG.error("Error parsing JOIN_TIME_OUT hint - invalid number format: {}", e.getMessage());
    } catch (Exception e) {
      LOG.error("Unexpected error parsing JOIN_TIME_OUT hint from SQL: {}", e.getMessage());
    }

    return Optional.empty();
  }
}
