/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.RareTopDigest;

@Value.Enclosing
public class RareTopPushdownRule extends InterruptibleRelRule<RareTopPushdownRule.Config>
    implements SubstitutionRule {

  protected RareTopPushdownRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final LogicalProject project = call.rel(1);
    final CalciteLogicalIndexScan scan = call.rel(2);
    RareTopDigest digest;
    try {
      RexLiteral numberLiteral =
          (RexLiteral) ((RexCall) filter.getCondition()).getOperands().get(1);
      Integer number = numberLiteral.getValueAs(Integer.class);
      List<RexWindow> windows = PlanUtils.getRexWindowFromProject(project);
      if (windows.size() != 1) {
        return;
      }
      final List<String> fieldNameList = project.getInput().getRowType().getFieldNames();
      List<Integer> groupIndices = PlanUtils.getSelectColumns(windows.getFirst().partitionKeys);
      List<String> byList = groupIndices.stream().map(fieldNameList::get).toList();

      // ORDER BY must be either the count column alone, or the count column followed by the
      // rare/top target field ASC (the stable tie-break inserted by visitRareTopN). The pushdown
      // to the OpenSearch terms aggregation naturally tie-breaks on `_key:asc`, so the latter
      // shape lowers to the same wire request as the single-key shape.
      List<RexFieldCollation> orderKeys = windows.getFirst().orderKeys;
      if (orderKeys.isEmpty() || orderKeys.size() > 2) {
        return;
      }
      RexFieldCollation primaryOrderKey = orderKeys.getFirst();
      List<Integer> primaryOrderIndices =
          PlanUtils.getSelectColumns(List.of(primaryOrderKey.getKey()));
      List<String> primaryOrderList = primaryOrderIndices.stream().map(fieldNameList::get).toList();
      List<String> targetList =
          fieldNameList.stream()
              .filter(Predicate.not(byList::contains))
              .filter(Predicate.not(primaryOrderList::contains))
              .toList();
      if (targetList.size() != 1) {
        return;
      }
      String targetName = targetList.getFirst();
      if (hasIncompatibleTieBreak(orderKeys, fieldNameList, targetName)) {
        return;
      }
      digest = new RareTopDigest(targetName, byList, number, primaryOrderKey.getDirection());
    } catch (Exception e) {
      return;
    }
    CalciteLogicalIndexScan newScan = scan.pushDownRareTop(project, digest);
    if (newScan != null) {
      call.transformTo(newScan);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /**
   * Returns {@code true} when a second {@link RexFieldCollation} is present but is not the stable
   * tie-break shape this rule recognises (rare/top target field, ASC). A single-key shape (no
   * tie-break) is always accepted because the OpenSearch terms aggregation tie-breaks on {@code
   * _key:asc} natively. {@code orderKeys} arity is pre-checked by the caller.
   */
  private static boolean hasIncompatibleTieBreak(
      List<RexFieldCollation> orderKeys, List<String> fieldNameList, String targetName) {
    if (orderKeys.size() != 2) {
      return false;
    }
    RexFieldCollation tieBreakKey = orderKeys.get(1);
    if (tieBreakKey.getDirection() != RelFieldCollation.Direction.ASCENDING) {
      return true;
    }
    List<Integer> tieBreakIndices = PlanUtils.getSelectColumns(List.of(tieBreakKey.getKey()));
    List<String> tieBreakList = tieBreakIndices.stream().map(fieldNameList::get).toList();
    return tieBreakList.size() != 1 || !tieBreakList.getFirst().equals(targetName);
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    RareTopPushdownRule.Config DEFAULT =
        ImmutableRareTopPushdownRule.Config.builder()
            .build()
            .withDescription("Filter-Project(window)-TableScan(agg-pushed)")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalFilter.class)
                        .predicate(
                            filter -> filter.getCondition().getKind() == SqlKind.LESS_THAN_OR_EQUAL)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .predicate(PlanUtils::containsRowNumberRareTop)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(CalciteLogicalIndexScan.class)
                                                .predicate(
                                                    Predicate.not(
                                                        AbstractCalciteIndexScan
                                                            ::noAggregatePushed))
                                                .noInputs())));

    @Override
    default RareTopPushdownRule toRule() {
      return new RareTopPushdownRule(this);
    }
  }
}
