/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.Objects;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/**
 * Planner rule that push a {@link LogicalSort} with a semantic meaning of LIMIT ... [OFFSET ...]
 * down to {@link CalciteLogicalIndexScan}
 */
@Value.Enclosing
public class OpenSearchLimitIndexScanRule extends RelRule<OpenSearchLimitIndexScanRule.Config> {

  protected OpenSearchLimitIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalSort sort = call.rel(0);
    final CalciteLogicalIndexScan scan = call.rel(1);

    Integer limitValue = extractLimitValue(sort.fetch);
    Integer offsetValue = extractOffsetValue(sort.offset);
    if (limitValue != null && offsetValue != null) {
      CalciteLogicalIndexScan newScan = scan.pushDownLimit(limitValue, offsetValue);
      if (newScan != null) {
        call.transformTo(newScan);
      }
    }
  }

  private static Integer extractLimitValue(RexNode fetch) {
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
  private static Integer extractOffsetValue(RexNode offset) {
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
  public interface Config extends RelRule.Config {
    OpenSearchLimitIndexScanRule.Config DEFAULT =
        ImmutableOpenSearchLimitIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalSort.class)
                        .predicate(OpenSearchIndexScanRule::isLogicalSortLimit)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(OpenSearchIndexScanRule::noAggregatePushed)
                                    .noInputs()));

    @Override
    default OpenSearchLimitIndexScanRule toRule() {
      return new OpenSearchLimitIndexScanRule(this);
    }
  }
}
