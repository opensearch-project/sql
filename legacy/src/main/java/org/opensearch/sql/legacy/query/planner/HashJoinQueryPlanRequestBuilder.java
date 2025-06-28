/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner;

import java.util.Optional;
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

  /** Custom PIT keepalive timeout from JOIN_TIME_OUT hint */
  private Optional<TimeValue> customPitKeepAlive = Optional.empty();

  public HashJoinQueryPlanRequestBuilder(Client client, SqlRequest request) {
    this.client = client;
    this.request = request;
    this.config = new Config();
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
   * Set custom PIT keepalive timeout from JOIN_TIME_OUT hint
   *
   * @param timeout Custom timeout value (can be null)
   */
  public void setCustomPitKeepAlive(TimeValue timeout) {
    this.customPitKeepAlive = Optional.ofNullable(timeout);
    LOG.debug(
        "RequestBuilder: Set custom PIT keepalive to: {}",
        customPitKeepAlive.map(t -> t + " (" + t.getMillis() + "ms)").orElse("default"));
  }

  /**
   * Get custom PIT keepalive timeout if set
   *
   * @return Optional containing custom timeout, or empty if not set
   */
  public Optional<TimeValue> getCustomPitKeepAlive() {
    return customPitKeepAlive;
  }

  /**
   * Check if custom PIT keepalive is set
   *
   * @return true if custom timeout is configured
   */
  public boolean hasCustomPitKeepAlive() {
    return customPitKeepAlive.isPresent();
  }
}
