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
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.rule.MergeFilterAndFilter;
import org.opensearch.sql.planner.optimizer.rule.PushFilterUnderSort;
import org.opensearch.sql.planner.optimizer.rule.PushParseUnderFilter;

/**
 * {@link LogicalPlan} Optimizer.
 * The Optimizer will run in the TopDown manner.
 * 1> Optimize the current node with all the rules.
 * 2> Optimize the all the child nodes with all the rules.
 * 3) In case the child node could change, Optimize the current node again.
 */
public class LogicalPlanOptimizer {

  private final List<Rule<?>> rules;

  /**
   * Create {@link LogicalPlanOptimizer} with customized rules.
   */
  public LogicalPlanOptimizer(List<Rule<?>> rules) {
    this.rules = rules;
  }

  /**
   * Create {@link LogicalPlanOptimizer} with pre-defined rules.
   */
  public static LogicalPlanOptimizer create(DSL dsl) {
    return new LogicalPlanOptimizer(Arrays.asList(
        new MergeFilterAndFilter(dsl),
        new PushFilterUnderSort(),
        new PushParseUnderFilter()));
  }

  /**
   * Optimize {@link LogicalPlan}.
   */
  public LogicalPlan optimize(LogicalPlan plan) {
    LogicalPlan optimized = internalOptimize(plan);
    optimized.replaceChildPlans(
        optimized.getChild().stream().map(this::optimize).collect(
            Collectors.toList()));
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
          done = false;
        }
      }
    }
    return node;
  }
}
