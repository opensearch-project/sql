/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_DEDUP;

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
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
    // TODO Used when number of duplication is more than 1
    final LogicalFilter numOfDedupFilter = call.rel(1);
    final LogicalProject projectWithWindow = call.rel(2);
    final CalciteLogicalIndexScan scan = call.rel(3);
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

  private static boolean validFilter(LogicalFilter filter) {
    if (filter.getCondition().getKind() != SqlKind.LESS_THAN_OR_EQUAL) {
      return false;
    }
    List<RexNode> operandsOfCondition = ((RexCall) filter.getCondition()).getOperands();
    RexNode leftOperand = operandsOfCondition.getFirst();
    if (!(leftOperand instanceof RexInputRef ref)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup since the left operand is not RexInputRef");
      }
      return false;
    }
    String referenceName = filter.getRowType().getFieldNames().get(ref.getIndex());
    if (!referenceName.equals(ROW_NUMBER_COLUMN_FOR_DEDUP)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot pushdown the dedup since the left operand is not {}",
            ROW_NUMBER_COLUMN_FOR_DEDUP);
      }
      return false;
    }
    RexNode rightOperand = operandsOfCondition.getLast();
    if (!(rightOperand instanceof RexLiteral numLiteral)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup since the right operand is not RexLiteral");
      }
      return false;
    }
    Integer num = numLiteral.getValueAs(Integer.class);
    if (num == null || num > 1) {
      // TODO leverage inner_hits for num > 1
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup since number of duplicate events is larger than 1");
      }
      return false;
    }
    return true;
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
                                    .predicate(OpenSearchDedupPushdownRule::validFilter)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalProject.class)
                                                .predicate(PlanUtils::containsRowNumberDedup)
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(CalciteLogicalIndexScan.class)
                                                            .predicate(
                                                                Predicate.not(
                                                                        AbstractCalciteIndexScan
                                                                            ::isLimitPushed)
                                                                    .and(
                                                                        AbstractCalciteIndexScan
                                                                            ::noAggregatePushed))
                                                            .noInputs()))));

    @Override
    default OpenSearchDedupPushdownRule toRule() {
      return new OpenSearchDedupPushdownRule(this);
    }
  }
}
