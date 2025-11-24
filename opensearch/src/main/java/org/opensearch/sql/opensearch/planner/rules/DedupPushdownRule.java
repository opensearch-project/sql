/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@Value.Enclosing
public class DedupPushdownRule extends RelRule<DedupPushdownRule.Config> {
  private static final Logger LOG = LogManager.getLogger();

  protected DedupPushdownRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject finalProject = call.rel(0);
    final LogicalFilter numOfDedupFilter = call.rel(1);
    final LogicalProject projectWithWindow = call.rel(2);
    if (call.rels.length == 4) {
      final CalciteLogicalIndexScan scan = call.rel(3);
      apply(call, finalProject, numOfDedupFilter, projectWithWindow, scan);
    } else if (call.rels.length == 6) {
      final LogicalProject projectWithCall = call.rel(4);
      final CalciteLogicalIndexScan scan = call.rel(5);
      apply(
          call,
          finalProject,
          numOfDedupFilter,
          projectWithWindow,
          //          Optional.of(projectWithCall),
          scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(
      RelOptRuleCall call,
      LogicalProject finalProject,
      LogicalFilter numOfDedupFilter,
      LogicalProject projectWithWindow,
      CalciteLogicalIndexScan scan) {
    List<RexWindow> windows = PlanUtils.getRexWindowFromProject(projectWithWindow);
    if (windows.size() != 1) {
      return;
    }

    List<RexNode> dedupColumns = windows.get(0).partitionKeys;
    if (dedupColumns.stream()
        .filter(rex -> rex.isA(SqlKind.INPUT_REF))
        .anyMatch(rex -> rex.getType().getSqlTypeName() == SqlTypeName.MAP)) {
      LOG.debug("Cannot pushdown the dedup since the dedup fields contains MAP type");
      // TODO https://github.com/opensearch-project/sql/issues/4564
      return;
    }
    if (projectWithWindow.getProjects().stream()
        .filter(rex -> !rex.isA(SqlKind.ROW_NUMBER))
        .filter(Predicate.not(dedupColumns::contains))
        .anyMatch(rex -> !rex.isA(SqlKind.INPUT_REF))) {
      // TODO fallback to the approach of Collapse search
      // | eval new_age = age + 1 | fields gender, new_age | dedup 1 gender
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot pushdown the dedup since the final outputs contain a column which is not"
                + " included in table schema");
      }
      return;
    }

    List<RexNode> rexCallsExceptWindow =
        projectWithWindow.getProjects().stream()
            .filter(rex -> !rex.isA(SqlKind.ROW_NUMBER))
            .filter(rex -> rex instanceof RexCall)
            .toList();
    if (!rexCallsExceptWindow.isEmpty()
        && dedupColumnsContainRexCall(rexCallsExceptWindow, dedupColumns)) {
      // TODO https://github.com/opensearch-project/sql/issues/4789
      // | eval new_gender = lower(gender) | fields new_gender, age | dedup 1 new_gender
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup since the dedup columns contain RexCall");
      }
      return;
    }

    // must be row_number <= number
    assert numOfDedupFilter.getCondition().isA(SqlKind.LESS_THAN_OR_EQUAL);
    RexLiteral literal =
        (RexLiteral) ((RexCall) numOfDedupFilter.getCondition()).getOperands().getLast();
    Integer dedupNumer = literal.getValueAs(Integer.class);

    // We convert the dedup pushdown to composite aggregate + top_hits:
    // Aggregate(literalAgg(dedupNumer), groups)
    // +- Project(groups, remaining)
    //    +- Scan
    // 1. Initial a RelBuilder to build aggregate by pushing Scan and Project
    RelBuilder relBuilder = call.builder();
    relBuilder.push(scan);
    relBuilder.push(projectWithWindow); // baseline the rowType to handle rename case
    Mapping mappingForDedupColumns =
        PlanUtils.mapping(dedupColumns, relBuilder.peek().getRowType());

    // 2. Push a Project which groups is first, then remaining finalOutput columns
    List<RexNode> reordered = new ArrayList<>(PlanUtils.getInputRefs(dedupColumns));
    projectWithWindow.getProjects().stream()
        .filter(rex -> !rex.isA(SqlKind.ROW_NUMBER))
        .filter(Predicate.not(dedupColumns::contains))
        .forEach(reordered::add);
    relBuilder.project(reordered, ImmutableList.of(), true);
    // childProject includes all list of finalOutput columns
    LogicalProject childProject = (LogicalProject) relBuilder.peek();

    // 3. Push an Aggregate
    // We push down a LITERAL_AGG with dedupNumer for converting the dedup command to aggregate:
    // (1) Pass the dedupNumer to AggregateAnalyzer.processAggregateCalls()
    // (2) Distinguish it from an optimization operator and user defined aggregator.
    // (LITERAL_AGG is used in optimization normally, see {@link SqlKind#LITERAL_AGG})
    final List<RexNode> newDedupColumns = RexUtil.apply(mappingForDedupColumns, dedupColumns);
    relBuilder.aggregate(relBuilder.groupKey(newDedupColumns), relBuilder.literalAgg(dedupNumer));
    PlanUtils.addIgnoreNullBucketHintToAggregate(relBuilder);
    // peek the aggregate after hint being added
    LogicalAggregate aggregate = (LogicalAggregate) relBuilder.build();

    CalciteLogicalIndexScan newScan =
        (CalciteLogicalIndexScan) scan.pushDownAggregate(aggregate, childProject);
    if (newScan != null) {
      // reorder back to original order
      call.transformTo(newScan.copyWithNewSchema(finalProject.getRowType()));
    }
  }

  private static boolean dedupColumnsContainRexCall(
      List<RexNode> calls, List<RexNode> dedupColumns) {
    List<Integer> dedupColumnIndicesFromCall =
        PlanUtils.getSelectColumns(calls).stream().distinct().toList();
    List<Integer> dedupColumnsIndicesFromPartitionKeys =
        PlanUtils.getSelectColumns(dedupColumns).stream().distinct().toList();
    return dedupColumnsIndicesFromPartitionKeys.stream()
        .anyMatch(dedupColumnIndicesFromCall::contains);
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    // Can only push the case with KEEPEMPTY=false:
    // +- LogicalProject(no _row_number_dedup_)
    //    +- LogicalFilter(condition contains _row_number_dedup_)
    //       +- LogicalProject(contains _row_number_dedup_)
    //          +- CalciteLogicalIndexScan
    Config DEFAULT =
        ImmutableDedupPushdownRule.Config.builder()
            .build()
            .withDescription("Dedup-to-Aggregate")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .predicate(Predicate.not(PlanUtils::containsRowNumberDedup))
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(Config::validDedupNumberChecker)
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
                                                                            ::noAggregatePushed)
                                                                    .and(
                                                                        AbstractCalciteIndexScan
                                                                            ::isProjectPushed))
                                                            .noInputs()))));
    // +- LogicalProject(no _row_number_dedup_)
    //    +- LogicalFilter(condition contains _row_number_dedup_)
    //       +- LogicalProject(contains _row_number_dedup_)
    //          +- LogicalFilter(condition IS NOT NULL(dedupColumn)) // could be optimized similar
    // to https://github.com/opensearch-project/sql/issues/4811
    //             +- LogicalProject(dedupColumn is call)
    //                +- CalciteLogicalIndexScan
    Config DEDUP_EXPR =
        ImmutableDedupPushdownRule.Config.builder()
            .build()
            .withDescription("DedupWithExpression-to-Aggregate")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .predicate(Predicate.not(PlanUtils::containsRowNumberDedup))
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(Config::validDedupNumberChecker)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalProject.class)
                                                .predicate(PlanUtils::containsRowNumberDedup)
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(LogicalFilter.class)
                                                            .predicate(Config::isNotNull)
                                                            .oneInput(
                                                                b4 ->
                                                                    b4.operand(LogicalProject.class)
                                                                        .predicate(
                                                                            PlanUtils
                                                                                ::containsRexCall)
                                                                        .oneInput(
                                                                            b5 ->
                                                                                b5.operand(
                                                                                        CalciteLogicalIndexScan
                                                                                            .class)
                                                                                    .predicate(
                                                                                        Predicate
                                                                                            .not(
                                                                                                AbstractCalciteIndexScan
                                                                                                    ::isLimitPushed)
                                                                                            .and(
                                                                                                AbstractCalciteIndexScan
                                                                                                    ::noAggregatePushed)
                                                                                            .and(
                                                                                                AbstractCalciteIndexScan
                                                                                                    ::isProjectPushed))
                                                                                    .noInputs()))))));

    @Override
    default DedupPushdownRule toRule() {
      return new DedupPushdownRule(this);
    }

    private static boolean validDedupNumberChecker(LogicalFilter filter) {
      return filter.getCondition().isA(SqlKind.LESS_THAN_OR_EQUAL)
          && PlanUtils.containsRowNumberDedup(filter);
    }

    private static boolean isNotNull(LogicalFilter filter) {
      return filter.getCondition().isA(SqlKind.IS_NOT_NULL);
    }
  }
}
