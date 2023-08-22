/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.Plan;
import org.opensearch.sql.legacy.query.planner.core.PlanNode.Visitor;
import org.opensearch.sql.legacy.query.planner.logical.LogicalPlan;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Estimation;
import org.opensearch.sql.legacy.query.planner.resource.ResourceManager;

/** Physical plan */
public class PhysicalPlan implements Plan {

  private static final Logger LOG = LogManager.getLogger();

  /** Optimized logical plan that being ready for physical planning */
  private final LogicalPlan logicalPlan;

  /** Root of physical plan tree */
  private PhysicalOperator<SearchHit> root;

  public PhysicalPlan(LogicalPlan logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  @Override
  public void traverse(Visitor visitor) {
    if (root != null) {
      root.accept(visitor);
    }
  }

  @Override
  public void optimize() {
    Estimation<SearchHit> estimation = new Estimation<>();
    logicalPlan.traverse(estimation);
    root = estimation.optimalPlan();
  }

  /** Execute physical plan after verifying if system is healthy at the moment */
  public List<SearchHit> execute(ExecuteParams params) {
    if (shouldReject(params)) {
      throw new IllegalStateException("Query request rejected due to insufficient resource");
    }

    try (PhysicalOperator<SearchHit> op = root) {
      return doExecutePlan(op, params);
    } catch (Exception e) {
      LOG.error("Error happened during execution", e);
      // Runtime error or circuit break. Should we return partial result to customer?
      throw new IllegalStateException("Error happened during execution", e);
    }
  }

  /** Reject physical plan execution of new query request if unhealthy */
  private boolean shouldReject(ExecuteParams params) {
    return !((ResourceManager) params.get(ExecuteParams.ExecuteParamType.RESOURCE_MANAGER))
        .isHealthy();
  }

  /** Execute physical plan in order: open, fetch result, close */
  private List<SearchHit> doExecutePlan(PhysicalOperator<SearchHit> op, ExecuteParams params)
      throws Exception {
    List<SearchHit> hits = new ArrayList<>();
    op.open(params);

    while (op.hasNext()) {
      hits.add(op.next().data());
    }

    if (LOG.isTraceEnabled()) {
      hits.forEach(hit -> LOG.trace("Final result row: {}", hit.getSourceAsMap()));
    }
    return hits;
  }
}
