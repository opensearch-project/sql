/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;

/**
 * Base class for OpenSearch planner rules that automatically checks for thread interruption during
 * query planning. This ensures that long-running planning operations can be interrupted when a
 * query timeout occurs.
 *
 * <p>All OpenSearch planner rules should extend this class instead of extending {@link RelRule}
 * directly. This provides automatic timeout support without requiring manual interruption checks in
 * each rule.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * public class MyCustomRule extends InterruptibleRelRule<MyCustomRule.Config> {
 *   protected MyCustomRule(Config config) {
 *     super(config);
 *   }
 *
 *   @Override
 *   protected void onMatchImpl(RelOptRuleCall call) {
 *     // Rule implementation - interruption is checked automatically
 *     // before this method is called
 *   }
 * }
 * }</pre>
 *
 * @param <C> the configuration type for this rule
 */
public abstract class InterruptibleRelRule<C extends OpenSearchRuleConfig> extends RelRule<C> {

  /**
   * Constructs an InterruptibleRelRule with the given configuration.
   *
   * @param config the rule configuration
   */
  protected InterruptibleRelRule(C config) {
    super(config);
  }

  /**
   * Called when the rule matches. This method checks for thread interruption before delegating to
   * the implementation-specific {@link #onMatchImpl(RelOptRuleCall)} method.
   *
   * <p>Do not override this method in subclasses. Instead, override {@link
   * #onMatchImpl(RelOptRuleCall)}.
   *
   * @param call the rule call context
   * @throws RuntimeException wrapping {@link InterruptedException} if the thread has been
   *     interrupted
   */
  @Override
  public final void onMatch(RelOptRuleCall call) {
    if (Thread.currentThread().isInterrupted()) {
      throw new OpenSearchTimeoutException(
          new InterruptedException(
              "Query planning interrupted in rule: " + getClass().getSimpleName()));
    }

    onMatchImpl(call);
  }

  /**
   * Implementation-specific match handler. Subclasses must implement this method instead of
   * overriding {@link #onMatch(RelOptRuleCall)}.
   *
   * <p>This method is called after an automatic interruption check. If the thread has been
   * interrupted (due to a timeout), this method will not be called.
   *
   * @param call the rule call context
   */
  protected abstract void onMatchImpl(RelOptRuleCall call);
}
