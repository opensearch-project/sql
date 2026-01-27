/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.Objects;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/**
 * Planner rule that push a {@link LogicalSort} with a semantic meaning of LIMIT ... [OFFSET ...]
 * down to {@link CalciteLogicalIndexScan}
 */
@Value.Enclosing
public class LimitIndexScanRule extends InterruptibleRelRule<LimitIndexScanRule.Config> {

  protected LimitIndexScanRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final LogicalSort sort = call.rel(0);
    final CalciteLogicalIndexScan scan = call.rel(1);

    // If collation mismatch, it means this LogicalSort is actually a combination of sort and limit
    // Then we cannot operate limit push down directly
    if (!sort.getCollation().getFieldCollations().isEmpty()
        && sort.getCollation() != scan.getTraitSet().getCollation()) {
      return;
    }

    Integer limitValue = extractLimitValue(sort.fetch);
    Integer offsetValue = extractOffsetValue(sort.offset);
    if (limitValue != null && offsetValue != null) {
      AbstractRelNode newOperator = scan.pushDownLimit(sort, limitValue, offsetValue);
      if (newOperator != null) {
        call.transformTo(newOperator);
        PlanUtils.tryPruneRelNodes(call);
      }
    }
  }

  public static Integer extractLimitValue(RexNode fetch) {
    // fetch is always a integer literal (specified in our PPL/SQL syntax)
    if (fetch instanceof RexLiteral) {
      return ((RexLiteral) fetch).getValueAs(Integer.class);
    }
    return null;
  }

  /**
   * Extracts the offset value from the given <code>RexNode</code>. If the offset is <code>null
   * </code>, it defaults to 0. For example:
   *
   * <ul>
   *   <li><code>source=people | head 1</code> will have a <code>null</code> offset, which is
   *       converted to 0.
   *   <li><code>source=people | head 1 from 2</code> will have an offset of 2.
   * </ul>
   *
   * @param offset The <code>RexNode</code> representing the offset.
   * @return The extracted offset value, or <code>null</code> if it cannot be determined.
   */
  public static Integer extractOffsetValue(RexNode offset) {
    if (Objects.isNull(offset)) {
      return 0;
    }
    if (offset instanceof RexLiteral) {
      return ((RexLiteral) offset).getValueAs(Integer.class);
    }
    return null;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    LimitIndexScanRule.Config DEFAULT =
        ImmutableLimitIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalSort.class)
                        .predicate(PlanUtils::isLogicalSortLimit)
                        .oneInput(b1 -> b1.operand(CalciteLogicalIndexScan.class).noInputs()));

    @Override
    default LimitIndexScanRule toRule() {
      return new LimitIndexScanRule(this);
    }
  }
}
