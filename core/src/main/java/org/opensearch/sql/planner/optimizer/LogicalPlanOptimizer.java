/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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
        new PushFilterUnderSort()));
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
