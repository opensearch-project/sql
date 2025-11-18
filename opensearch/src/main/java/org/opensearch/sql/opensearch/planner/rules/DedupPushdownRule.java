/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
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
      System.out.println("dedup " + numOfDedupFilter);
      final CalciteLogicalIndexScan scan = call.rel(3);
      apply(call, finalProject, numOfDedupFilter, projectWithWindow, scan);
    } else if (call.rels.length == 5) {
      final LogicalProject project = call.rel(4);
      final CalciteLogicalIndexScan scan = call.rel(5);
      apply(call, finalProject, numOfDedupFilter, projectWithWindow, scan);
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
    if (projectWithWindow.getProjects().stream()
        .filter(rex -> !rex.isA(SqlKind.ROW_NUMBER))
        .filter(Predicate.not(dedupColumns::contains))
        .anyMatch(rex -> !rex.isA(SqlKind.INPUT_REF))) {
      // We can push down:
      // | eval new_gender = lower(gender) | fields new_gender, age | dedup 1 new_gender
      // But cannot push down:
      // | eval new_age = age + 1 | fields gender, new_age | dedup 1 gender
      // TODO fallback to the approach of Collapse search
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot pushdown the dedup since the final outputs contain a column which is not"
                + " included in table schema");
      }
      return;
    }

    // Initialization for building Aggregate
    RelBuilder relBuilder = call.builder();
    relBuilder.push(scan);
    Mapping mappingForDedupColumns =
        PlanUtils.mapping(dedupColumns, relBuilder.peek().getRowType());
    // Create new Project: grouping first, then remaining finalOutput columns
    List<RexNode> reordered = new ArrayList<>(PlanUtils.getInputRefs(dedupColumns));
    projectWithWindow.getProjects().stream()
        .filter(rex -> !rex.isA(SqlKind.ROW_NUMBER))
        .filter(Predicate.not(dedupColumns::contains))
        .forEach(reordered::add);
    relBuilder.project(reordered);
    // childProject includes all list of finalOutput columns
    LogicalProject childProject = (LogicalProject) relBuilder.peek();

    final List<RexNode> newDedupColumns = RexUtil.apply(mappingForDedupColumns, dedupColumns);
    // Create the Aggregate for pushdown.
    relBuilder.aggregate(relBuilder.groupKey(newDedupColumns), relBuilder.literalAgg("TopHits"));

    List<RexNode> predicates = RelOptUtil.disjunctions(numOfDedupFilter.getCondition());
    if (predicates.size() == 1) {
      // KEEPEMPTY=false
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
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    // Can only push the case with KEEPEMPTY=false:
    // +- LogicalProject(no _row_number_dedup_)
    //    +- LogicalFilter(condition contains _row_number_dedup_)
    //       +- LogicalProject(contains _row_number_dedup_, no call)
    //          +- CalciteLogicalIndexScan
    Config DEFAULT =
        ImmutableDedupPushdownRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .predicate(Predicate.not(PlanUtils::containsRowNumberDedup))
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(Config::validFilter)
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
    //       +- LogicalProject(contains _row_number_dedup_, not contains call)
    //          +- LogicalProject(contains call) <- cannot pushdown script project
    //                +- CalciteLogicalIndexScan
    Config DEDUP_SCRIPT =
        ImmutableDedupPushdownRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .predicate(Predicate.not(PlanUtils::containsRowNumberDedup))
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(Config::validFilter)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalProject.class)
                                                .predicate(PlanUtils::containsRowNumberDedup)
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(LogicalProject.class)
                                                            .predicate(PlanUtils::containsRexCall)
                                                            .oneInput(
                                                                b4 ->
                                                                    b4.operand(
                                                                            CalciteLogicalIndexScan
                                                                                .class)
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
                                                                        .noInputs())))));

    @Override
    default DedupPushdownRule toRule() {
      return new DedupPushdownRule(this);
    }

    private static boolean validFilter(LogicalFilter filter) {
      try {
        filter
            .getCondition()
            .accept(
                new RexVisitorImpl<Void>(true) {
                  @Override
                  public Void visitCall(RexCall call) {
                    if (call.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
                      List<RexNode> operandsOfCondition = call.getOperands();
                      RexNode rightOperand = operandsOfCondition.getLast();
                      if (!(rightOperand instanceof RexLiteral numLiteral)) {
                        throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
                            "Cannot pushdown the dedup since the right operand is not RexLiteral");
                      }
                      Integer num = numLiteral.getValueAs(Integer.class);
                      if (num == null || num > 1) {
                        // TODO https://github.com/opensearch-project/sql/issues/4789
                        // support number of duplication more than 1
                        throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
                            "Cannot pushdown the dedup since number of duplicate events is larger"
                                + " than 1");
                      }
                    } else {
                      // Only support the case with KEEPEMPTY=false
                      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
                          "Cannot pushdown the dedup since the condition type is "
                              + call.getKind());
                    }
                    return null;
                  }
                });
        return PlanUtils.containsRowNumberDedup(filter);
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) LOG.debug(e.getMessage());
        return false;
      }
    }
  }
}
