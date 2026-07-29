/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import static org.opensearch.sql.calcite.utils.PlanUtils.aggIgnoreNullBucket;
import static org.opensearch.sql.calcite.utils.PlanUtils.maybeTimeSpanAgg;
import static org.opensearch.sql.expression.function.PPLBuiltinOperators.WIDTH_BUCKET;

import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.expression.function.udf.binning.WidthBucketFunction;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Planner rule that push a {@link LogicalAggregate} down to {@link CalciteLogicalIndexScan} */
@Value.Enclosing
public class AggregateIndexScanRule extends InterruptibleRelRule<AggregateIndexScanRule.Config> {

  /** Creates a AggregateIndexScanRule. */
  protected AggregateIndexScanRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    if (call.rels.length == 5) {
      final LogicalAggregate aggregate = call.rel(0);
      final LogicalProject topProject = call.rel(1);
      final LogicalFilter filter = call.rel(2);
      final LogicalProject bottomProject = call.rel(3);
      final CalciteLogicalIndexScan scan = call.rel(4);
      if (PlanUtils.isNotNullDerivedFromAgg(filter.getCondition(), aggregate, topProject, null)) {
        final List<RexNode> newProjects =
            RelOptUtil.pushPastProjectUnlessBloat(
                topProject.getProjects(), bottomProject, RelOptUtil.DEFAULT_BLOAT);
        if (newProjects != null) {
          // replace the two projects with a combined projection
          RelBuilder relBuilder = call.builder();
          relBuilder.push(scan);
          relBuilder.project(newProjects, topProject.getRowType().getFieldNames());
          RelNode node = relBuilder.build();
          if (node instanceof LogicalProject newProject) {
            apply(call, aggregate, newProject, scan);
          } else if (node.equals(scan)) {
            // It means no project is needed
            apply(call, aggregate, null, scan);
          }
          // Do nothing, no any transform
        }
      }
    } else if (call.rels.length == 4) {
      final LogicalAggregate aggregate = call.rel(0);
      final LogicalFilter filter = call.rel(1);
      final LogicalProject project = call.rel(2);
      final CalciteLogicalIndexScan scan = call.rel(3);
      if (PlanUtils.isNotNullDerivedFromAgg(filter.getCondition(), aggregate, project, null)) {
        // Try to do the aggregate push down and ignore the filter if the filter sources from the
        // aggregate's hint. See{@link CalciteRelNodeVisitor::visitAggregation}
        apply(call, aggregate, project, scan);
      }
    } else if (call.rels.length == 3) {
      final LogicalAggregate aggregate = call.rel(0);
      final LogicalProject project = call.rel(1);
      final CalciteLogicalIndexScan scan = call.rel(2);
      apply(call, aggregate, project, scan);
    } else if (call.rels.length == 2) {
      final LogicalAggregate aggregate = call.rel(0);
      final CalciteLogicalIndexScan scan = call.rel(1);
      apply(call, aggregate, null, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(
      RelOptRuleCall call,
      LogicalAggregate aggregate,
      @Nullable LogicalProject project,
      CalciteLogicalIndexScan scan) {
    AbstractRelNode newRelNode = scan.pushDownAggregate(aggregate, project);
    if (newRelNode != null) {
      call.transformTo(newRelNode);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    Config DEFAULT =
        ImmutableAggregateIndexScanRule.Config.builder()
            .build()
            .withDescription("Agg-Project-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .predicate(
                                        // Support push down aggregate with project that:
                                        // 1. No RexOver and no duplicate projection
                                        // 2. Contains width_bucket function on date field referring
                                        // to bin command with parameter bins
                                        Predicate.not(LogicalProject::containsOver)
                                            .and(PlanUtils::distinctProjectList)
                                            .or(Config::containsWidthBucketFuncOnDate))
                                    .oneInput(
                                        b2 ->
                                            b2.operand(CalciteLogicalIndexScan.class)
                                                .predicate(
                                                    Predicate.not(
                                                            AbstractCalciteIndexScan::isLimitPushed)
                                                        .and(
                                                            AbstractCalciteIndexScan
                                                                ::noAggregatePushed))
                                                .noInputs())));
    Config AGGREGATE_SCAN =
        ImmutableAggregateIndexScanRule.Config.builder()
            .build()
            .withDescription("Agg-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        Predicate.not(AbstractCalciteIndexScan::isLimitPushed)
                                            .and(AbstractCalciteIndexScan::noAggregatePushed))
                                    .noInputs()));

    Config BUCKET_NON_NULL_AGG =
        ImmutableAggregateIndexScanRule.Config.builder()
            .build()
            .withDescription("Agg-Filter-Project-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .predicate(aggIgnoreNullBucket)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(PlanUtils::mayBeFilterFromBucketNonNull)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalProject.class)
                                                .predicate(
                                                    // Support push down aggregate with project
                                                    // that:
                                                    // 1. No RexOver and no duplicate projection
                                                    // 2. Contains width_bucket function on date
                                                    // field referring
                                                    // to bin command with parameter bins
                                                    Predicate.not(LogicalProject::containsOver)
                                                        .and(PlanUtils::distinctProjectList)
                                                        .or(Config::containsWidthBucketFuncOnDate))
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

    Config BUCKET_NON_NULL_AGG_WITH_UDF =
        ImmutableAggregateIndexScanRule.Config.builder()
            .build()
            .withDescription("Agg-Project-Filter-Project-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .predicate(aggIgnoreNullBucket.or(maybeTimeSpanAgg))
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .predicate(
                                        Predicate.not(LogicalProject::containsOver)
                                            .and(PlanUtils::distinctProjectList))
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalFilter.class)
                                                .predicate(PlanUtils::mayBeFilterFromBucketNonNull)
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(LogicalProject.class)
                                                            .predicate(
                                                                Predicate.not(
                                                                        LogicalProject
                                                                            ::containsOver)
                                                                    .and(
                                                                        PlanUtils
                                                                            ::distinctProjectList)
                                                                    .or(
                                                                        Config
                                                                            ::containsWidthBucketFuncOnDate))
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
                                                                                        ::noAggregatePushed))
                                                                        .noInputs())))));

    @Override
    default AggregateIndexScanRule toRule() {
      return new AggregateIndexScanRule(this);
    }

    static boolean containsWidthBucketFuncOnDate(LogicalProject project) {
      return project.getProjects().stream()
          .anyMatch(
              expr ->
                  expr instanceof RexCall rexCall
                      && rexCall.getOperator().equals(WIDTH_BUCKET)
                      && WidthBucketFunction.dateRelatedType(
                          rexCall.getOperands().getFirst().getType()));
    }
  }
}
