/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule;

import static org.opensearch.sql.planner.optimizer.pattern.Patterns.evalCapture;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.limit;
import static org.opensearch.sql.planner.optimizer.rule.EvalPushDown.EvalPushDownBuilder.match;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.pattern.CapturePattern;
import com.facebook.presto.matching.pattern.WithPattern;
import java.util.List;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Rule template for all rules related to push down logical plans under eval, so these plans can
 * avoid blocking by eval and may have chances to be pushed down into table scan by rules in {@link
 * org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown}.
 */
public class EvalPushDown<T extends LogicalPlan> implements Rule<T> {

  // TODO: Add more rules to push down sort and project
  /** Push down optimize rule for limit operator. Transform `limit -> eval` to `eval -> limit` */
  public static final Rule<LogicalLimit> PUSH_DOWN_LIMIT =
      match(limit(evalCapture()))
          .apply(
              (limit, logicalEval) -> {
                List<LogicalPlan> child = logicalEval.getChild();
                limit.replaceChildPlans(child);
                logicalEval.replaceChildPlans(List.of(limit));
                return logicalEval;
              });

  private final Capture<LogicalEval> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<T> pattern;

  private final BiFunction<T, LogicalEval, LogicalPlan> pushDownFunction;

  @SuppressWarnings("unchecked")
  public EvalPushDown(
      WithPattern<T> pattern, BiFunction<T, LogicalEval, LogicalPlan> pushDownFunction) {
    this.pattern = pattern;
    this.capture = ((CapturePattern<LogicalEval>) pattern.getPattern()).capture();
    this.pushDownFunction = pushDownFunction;
  }

  @Override
  public LogicalPlan apply(T plan, Captures captures) {
    LogicalEval logicalEval = captures.get(capture);
    return pushDownFunction.apply(plan, logicalEval);
  }

  static class EvalPushDownBuilder<T extends LogicalPlan> {

    private WithPattern<T> pattern;

    public static <T extends LogicalPlan> EvalPushDown.EvalPushDownBuilder<T> match(
        Pattern<T> pattern) {
      EvalPushDown.EvalPushDownBuilder<T> builder = new EvalPushDown.EvalPushDownBuilder<>();
      builder.pattern = (WithPattern<T>) pattern;
      return builder;
    }

    public EvalPushDown<T> apply(BiFunction<T, LogicalEval, LogicalPlan> pushDownFunction) {
      return new EvalPushDown<>(pattern, pushDownFunction);
    }
  }
}
