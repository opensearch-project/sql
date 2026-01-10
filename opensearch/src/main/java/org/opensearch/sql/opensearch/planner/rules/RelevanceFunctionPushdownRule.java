/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.MULTI_FIELDS_RELEVANCE_FUNCTION_SET;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.SINGLE_FIELD_RELEVANCE_FUNCTION_SET;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/**
 * Planner rule that always pushes down filters containing relevance functions (like query_string,
 * match, multi_match, etc.) to OpenSearch, regardless of the pushdown settings. This ensures
 * relevance functions are always executed by OpenSearch for optimal performance and functionality.
 */
@Value.Enclosing
public class RelevanceFunctionPushdownRule
    extends InterruptibleRelRule<RelevanceFunctionPushdownRule.Config> implements SubstitutionRule {

  /** Creates an RelevanceFunctionPushdownRule. */
  protected RelevanceFunctionPushdownRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      final LogicalFilter filter = call.rel(0);
      final CalciteLogicalIndexScan scan = call.rel(1);

      // This rule is only used when pushdown is disabled,
      // so we only push down filters that contain relevance functions
      if (containsRelevanceFunction(filter.getCondition())) {
        apply(call, filter, scan);
      }
    } else {
      throw new AssertionError(
          String.format("The length of rels should be 2 but got %s", call.rels.length));
    }
  }

  protected void apply(RelOptRuleCall call, Filter filter, CalciteLogicalIndexScan scan) {
    AbstractRelNode newRel = scan.pushDownFilter(filter);
    if (newRel != null) {
      call.transformTo(newRel);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /**
   * Checks if a RexNode contains any relevance functions.
   *
   * @param node The RexNode to check
   * @return true if the node contains relevance functions, false otherwise
   */
  private boolean containsRelevanceFunction(RexNode node) {
    RelevanceFunctionVisitor visitor = new RelevanceFunctionVisitor();
    node.accept(visitor);
    return visitor.hasRelevanceFunction();
  }

  /** Visitor to detect relevance functions in a RexNode tree. */
  private static class RelevanceFunctionVisitor extends RexVisitorImpl<Void> {
    private boolean foundRelevanceFunction = false;

    RelevanceFunctionVisitor() {
      super(true);
    }

    @Override
    public Void visitCall(RexCall call) {
      SqlOperator operator = call.getOperator();
      String operatorName = operator.getName().toLowerCase();

      // Check if this is a relevance function
      if (SINGLE_FIELD_RELEVANCE_FUNCTION_SET.contains(operatorName)
          || MULTI_FIELDS_RELEVANCE_FUNCTION_SET.contains(operatorName)) {
        foundRelevanceFunction = true;
        return null; // Stop traversing once we find a relevance function
      }

      // Continue traversing the tree
      return super.visitCall(call);
    }

    boolean hasRelevanceFunction() {
      return foundRelevanceFunction;
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    /** Config that matches Filter on CalciteLogicalIndexScan. */
    Config DEFAULT =
        ImmutableRelevanceFunctionPushdownRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalFilter.class)
                        .oneInput(b1 -> b1.operand(CalciteLogicalIndexScan.class).noInputs()));

    @Override
    default RelevanceFunctionPushdownRule toRule() {
      return new RelevanceFunctionPushdownRule(this);
    }
  }
}
