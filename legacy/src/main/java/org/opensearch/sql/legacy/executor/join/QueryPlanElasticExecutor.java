/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.join;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.SearchHit;
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

    if (request.getFirstTable().hasJoinTimeoutHint()
        || request.getSecondTable().hasJoinTimeoutHint()) {
      LOG.info("QueryPlanElasticExecutor: Using custom PIT keepalive from JOIN_TIME_OUT hint");
    } else {
      LOG.info(
          "QueryPlanElasticExecutor: No JOIN_TIME_OUT hint found, using default PIT keepalive");
    }

    this.queryPlanner = request.plan();
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
