/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.join;

import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.planner.HashJoinQueryPlanRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.QueryPlanner;
import org.opensearch.transport.client.Client;

/**
 * Executor for generic QueryPlanner execution. This executor is just acting as adaptor to integrate
 * with existing framework. In future, QueryPlanner should be executed by itself and leave the
 * response sent back or other post-processing logic to ElasticDefaultRestExecutor.
 */
class QueryPlanElasticExecutor extends ElasticJoinExecutor {

  private static final Logger LOG = LogManager.getLogger();

  private final QueryPlanner queryPlanner;

  QueryPlanElasticExecutor(Client client, HashJoinQueryPlanRequestBuilder request) {
    super(client, request);

    // Log the timeout configuration that was already parsed in the request builder
    if (request.getConfig().hasCustomPitKeepAlive()) {
      LOG.info(
          "QueryPlanElasticExecutor: Using custom PIT keepalive from JOIN_TIME_OUT hint: {}"
              + " seconds",
          request
              .getConfig()
              .getCustomPitKeepAlive()
              .map(timeout -> String.valueOf(timeout.getSeconds()))
              .orElse("unknown"));
    } else {
      LOG.info(
          "QueryPlanElasticExecutor: No JOIN_TIME_OUT hint found, using default PIT keepalive");
    }

    // Create the query planner (this will also set hint configs on table builders)
    this.queryPlanner = request.plan();
  }

  @Override
  public void run() throws IOException, SqlParseException {
    try {
      long timeBefore = System.currentTimeMillis();

      LOG.debug("QueryPlanElasticExecutor: Starting execution");

      results = innerRun();
      long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
      this.metaResults.setTookImMilli(joinTimeInMilli);

      LOG.debug("QueryPlanElasticExecutor: Completed execution in {} ms", joinTimeInMilli);
    } catch (Exception e) {
      LOG.error("Failed during QueryPlan join query run.", e);
      throw new IllegalStateException("Error occurred during QueryPlan join query run", e);
    }
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
