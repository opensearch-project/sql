/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical;

import java.util.Arrays;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeAggAndIndexScan;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeAggAndRelation;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeFilterAndRelation;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeLimitAndIndexScan;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeLimitAndRelation;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeSortAndIndexAgg;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeSortAndIndexScan;
import org.opensearch.sql.opensearch.planner.logical.rule.MergeSortAndRelation;
import org.opensearch.sql.opensearch.planner.logical.rule.PushProjectAndIndexScan;
import org.opensearch.sql.opensearch.planner.logical.rule.PushProjectAndRelation;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;

/**
 * OpenSearch storage specified logical plan optimizer.
 */
@UtilityClass
public class OpenSearchLogicalPlanOptimizerFactory {

  /**
   * Create OpenSearch storage specified logical plan optimizer.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(Arrays.asList(
        new MergeFilterAndRelation(),
        new MergeAggAndIndexScan(),
        new MergeAggAndRelation(),
        new MergeSortAndRelation(),
        new MergeSortAndIndexScan(),
        new MergeSortAndIndexAgg(),
        new MergeSortAndIndexScan(),
        // new MergeLimitAndRelation(),
        // new MergeLimitAndIndexScan(),
        new PushProjectAndRelation(),
        new PushProjectAndIndexScan()
    ));
  }
}
