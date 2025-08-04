/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@Value.Enclosing
public class OpenSearchDedupPushdownRule extends RelRule<OpenSearchDedupPushdownRule.Config> {
  private static final Logger LOG = LogManager.getLogger();

  protected OpenSearchDedupPushdownRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject finalOutput = call.rel(0);
    final LogicalFilter numOfDedupFilter = call.rel(1);
    final LogicalProject projectWithWindow = call.rel(2);
    final LogicalFilter isNotNullFilter = call.rel(3);
    final CalciteLogicalIndexScan scan = call.rel(4);
    RexLiteral numLiteral =
        PlanUtils.findLiterals(numOfDedupFilter.getCondition(), true).getFirst();
    Integer num = numLiteral.getValueAs(Integer.class);
    if (num == null || num > 1) {
      // TODO leverage inner_hits for num > 1
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup since number of duplicate events is larger than 1");
      }
      return;
    }
    List<RexWindow> windows = PlanUtils.getRexWindowFromProject(projectWithWindow);
    if (windows.isEmpty() || windows.stream().anyMatch(w -> w.partitionKeys.size() > 1)) {
      // TODO leverage inner_hits for multiple partition keys
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup with multiple fields");
      }
      return;
    }
    final List<String> fieldNameList = projectWithWindow.getInput().getRowType().getFieldNames();
    List<Integer> selectColumns = PlanUtils.getSelectColumns(windows.getFirst().partitionKeys);
    String fieldName = fieldNameList.get(selectColumns.getFirst());

    CalciteLogicalIndexScan newScan = scan.pushDownCollapse(finalOutput, fieldName);
    if (newScan != null) {
      call.transformTo(newScan);
    }
  }

  /**
   * Match fixed pattern:<br>
   * LogicalProject(remove _row_number_dedup_) <br>
   * LogicalFilter(condition=[<=($1, numOfDedup)]) <br>
   * LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $0)]) <br>
   * LogicalFilter(condition=[IS NOT NULL($0)]) <br>
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutableOpenSearchDedupPushdownRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(
                                        f ->
                                            f.getCondition().getKind()
                                                == SqlKind.LESS_THAN_OR_EQUAL)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalProject.class)
                                                .predicate(PlanUtils::containsRowNumberDedup)
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(LogicalFilter.class)
                                                            .predicate(
                                                                f ->
                                                                    f.getCondition().getKind()
                                                                        == SqlKind.IS_NOT_NULL)
                                                            .oneInput(
                                                                b4 ->
                                                                    b4.operand(
                                                                            CalciteLogicalIndexScan
                                                                                .class)
                                                                        .predicate(
                                                                            Predicate.not(
                                                                                    OpenSearchIndexScanRule
                                                                                        ::isLimitPushed)
                                                                                .and(
                                                                                    OpenSearchIndexScanRule
                                                                                        ::noAggregatePushed))
                                                                        .noInputs())))));

    @Override
    default OpenSearchDedupPushdownRule toRule() {
      return new OpenSearchDedupPushdownRule(this);
    }
  }
}
