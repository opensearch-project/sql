/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;

import com.facebook.presto.matching.Match;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.rule.MergeFilterAndFilter;
import org.opensearch.sql.planner.optimizer.rule.PushFilterUnderSort;
import org.opensearch.sql.planner.optimizer.rule.read.CreateTableScanBuilder;
import org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown;
import org.opensearch.sql.planner.optimizer.rule.write.CreateTableWriteBuilder;

/**
 * {@link LogicalPlan} Optimizer. The Optimizer will run in the TopDown manner. 1> Optimize the
 * current node with all the rules. 2> Optimize the all the child nodes with all the rules. 3) In
 * case the child node could change, Optimize the current node again.
 */
public class LogicalPlanOptimizer {

  private final List<Rule<?>> rules;

  /** Create {@link LogicalPlanOptimizer} with customized rules. */
  public LogicalPlanOptimizer(List<Rule<?>> rules) {
    this.rules = rules;
  }

  /** Create {@link LogicalPlanOptimizer} with pre-defined rules. */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(
        Arrays.asList(
            /*
             * Phase 1: Transformations that rely on relational algebra equivalence
             */
            new MergeFilterAndFilter(),
            new PushFilterUnderSort(),
            /*
             * Phase 2: Transformations that rely on data source push down capability
             */
            new CreateTableScanBuilder(),
            TableScanPushDown.PUSH_DOWN_FILTER,
            TableScanPushDown.PUSH_DOWN_AGGREGATION,
            TableScanPushDown.PUSH_DOWN_SORT,
            TableScanPushDown.PUSH_DOWN_LIMIT,
            new PushDownPageSize(),
            TableScanPushDown.PUSH_DOWN_HIGHLIGHT,
            TableScanPushDown.PUSH_DOWN_NESTED,
            TableScanPushDown.PUSH_DOWN_PROJECT,
            new CreateTableWriteBuilder()));
  }

  /** Optimize {@link LogicalPlan}. */
  public LogicalPlan optimize(LogicalPlan plan) {
    LogicalPlan optimized = internalOptimize(plan);
    optimized.replaceChildPlans(
        optimized.getChild().stream().map(this::optimize).collect(Collectors.toList()));
    return internalOptimize(optimized);
  }

  private LogicalPlan internalOptimize(LogicalPlan plan) {
    LogicalPlan node = plan;
    boolean done = false;
    while (!done) {
      done = true;
      for (Rule rule : rules) {
        Match match = DEFAULT_MATCHER.match(rule.pattern(), node);
        if (match.isPresent()) {
          node = rule.apply(match.value(), match.captures());

          // For new TableScanPushDown impl, pattern match doesn't necessarily cause
          // push down to happen. So reiterate all rules against the node only if the node
          // is actually replaced by any rule.
          // TODO: may need to introduce fixed point or maximum iteration limit in future
          if (node != match.value()) {
            done = false;
          }
        }
      }
    }
    return node;
  }
}
