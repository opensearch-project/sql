/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PPLHintUtils;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.utils.Utils;

@Value.Enclosing
public class DedupPushdownRule extends InterruptibleRelRule<DedupPushdownRule.Config>
    implements SubstitutionRule {
  private static final Logger LOG = LogManager.getLogger();

  protected DedupPushdownRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final LogicalProject finalProject = call.rel(0);
    // TODO Used when number of duplication is more than 1
    final LogicalFilter numOfDedupFilter = call.rel(1);
    final LogicalProject projectWithWindow = call.rel(2);
    final LogicalProject projectWithExpr = call.rel(4);
    final CalciteLogicalIndexScan scan = call.rel(5);
    apply(call, finalProject, numOfDedupFilter, projectWithWindow, projectWithExpr, scan);
  }

  protected void apply(
      RelOptRuleCall call,
      LogicalProject finalProject,
      LogicalFilter numOfDedupFilter,
      LogicalProject projectWithWindow,
      LogicalProject bottomProject,
      CalciteLogicalIndexScan scan) {
    List<RexWindow> windows = PlanUtils.getRexWindowFromProject(projectWithWindow);
    if (windows.size() != 1) {
      return;
    }

    List<RexNode> dedupColumns = windows.get(0).partitionKeys;
    if (dedupColumns.stream()
        .filter(rex -> rex.isA(SqlKind.INPUT_REF))
        .anyMatch(
            rex ->
                rex.getType().getSqlTypeName() == SqlTypeName.MAP
                    || rex.getType().getSqlTypeName() == SqlTypeName.ARRAY)) {
      // TODO https://github.com/opensearch-project/sql/issues/5006
      LOG.debug("Cannot pushdown the dedup since the dedup fields contains MAP/ARRAY type");
      // fallback to non-pushdown
      return;
    }
    // must be row_number <= number
    assert numOfDedupFilter.getCondition().isA(SqlKind.LESS_THAN_OR_EQUAL);
    RexLiteral literal =
        (RexLiteral) ((RexCall) numOfDedupFilter.getCondition()).getOperands().getLast();
    Integer dedupNumer = literal.getValueAs(Integer.class);

    RelBuilder relBuilder = call.builder();
    relBuilder.push(bottomProject);

    // 1 Build the target projections
    // 1.1 The dedup columns should be put first as it will be used in the aggregation below;
    // The dedup columns may source from the current projectWithWindow or the bottomProject
    // 1.2 The rest of the columns should be put after the dedup columns, which is force needed in
    // dedup.
    List<Pair<RexNode, String>> targetProjections = new ArrayList<>();
    for (RexNode dedupColumn : dedupColumns) {
      if (projectWithWindow.getProjects().contains(dedupColumn)) {
        targetProjections.add(
            projectWithWindow
                .getNamedProjects()
                .get(projectWithWindow.getProjects().indexOf(dedupColumn)));
      } else if (dedupColumn instanceof RexInputRef ref) {
        targetProjections.add(
            Pair.of(
                dedupColumn, relBuilder.peek().getRowType().getFieldNames().get(ref.getIndex())));
      } else {
        LOG.warn("The dedup column {} is illegal.", dedupColumn);
        return;
      }
    }
    if (targetProjections.stream()
        .anyMatch(
            pair ->
                Utils.resolveNestedPath(pair.getValue(), scan.getOsIndex().getFieldTypes())
                    != null)) {
      // fallback to non-pushdown if the dedup columns contain nested fields.
      return;
    }
    for (Pair<RexNode, String> project : projectWithWindow.getNamedProjects()) {
      if (!project.getKey().isA(SqlKind.ROW_NUMBER) && !targetProjections.contains(project)) {
        targetProjections.add(project);
      }
    }

    relBuilder.project(
        targetProjections.stream().map(Pair::getKey).toList(),
        targetProjections.stream().map(Pair::getValue).toList());
    LogicalProject targetChildProject = (LogicalProject) relBuilder.peek();

    // 2 Push an Aggregate
    // We push down a LITERAL_AGG with dedupNumer for converting the dedup command to aggregate:
    // (1) Pass the dedupNumer to AggregateAnalyzer.processAggregateCalls()
    // (2) Distinguish it from an optimization operator and user defined aggregator.
    // (LITERAL_AGG is used in optimization normally, see {@link SqlKind#LITERAL_AGG})
    List<Integer> newGroupByList = IntStream.range(0, dedupColumns.size()).boxed().toList();
    relBuilder.aggregate(
        relBuilder.groupKey(relBuilder.fields(newGroupByList)), relBuilder.literalAgg(dedupNumer));

    // add bucket_nullable = false hint
    PPLHintUtils.addIgnoreNullBucketHintToAggregate(relBuilder);
    // peek the aggregate after hint being added
    LogicalAggregate aggregate = (LogicalAggregate) relBuilder.build();
    assert aggregate.getGroupSet().asList().equals(newGroupByList)
        : "The group set of aggregate should be exactly the same as the generated group list";

    CalciteLogicalIndexScan newScan =
        (CalciteLogicalIndexScan) scan.pushDownAggregate(aggregate, targetChildProject);
    if (newScan != null) {
      // Back to original project order
      call.transformTo(newScan.copyWithNewSchema(finalProject.getRowType()));
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    // +- LogicalProject(no _row_number_dedup_)
    //    +- LogicalFilter(condition contains _row_number_dedup_)
    //       +- LogicalProject(contains _row_number_dedup_)
    //          +- LogicalFilter(condition IS NOT NULL(dedupColumn))
    //             +- LogicalProject(dedupColumn is call or ref)
    //                +- CalciteLogicalIndexScan
    Config DEFAULT =
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
                                                            .predicate(
                                                                PlanUtils
                                                                    ::mayBeFilterFromBucketNonNull)
                                                            .oneInput(
                                                                b4 ->
                                                                    b4.operand(LogicalProject.class)
                                                                        .oneInput(
                                                                            b5 ->
                                                                                b5.operand(
                                                                                        CalciteLogicalIndexScan
                                                                                            .class)
                                                                                    .predicate(
                                                                                        Config
                                                                                            ::tableScanChecker)
                                                                                    .noInputs()))))));

    /**
     * Project must be not pushed since the name of expression would lose after project pushed. E.g.
     * in query "eval new_a = a + 1 | dedup b", the "new_a" will lose.
     */
    private static boolean tableScanChecker(AbstractCalciteIndexScan scan) {
      return Predicate.not(AbstractCalciteIndexScan::isLimitPushed)
          .and(AbstractCalciteIndexScan::noAggregatePushed)
          .test(scan);
    }

    @Override
    default DedupPushdownRule toRule() {
      return new DedupPushdownRule(this);
    }

    private static boolean validDedupNumberChecker(LogicalFilter filter) {
      return filter.getCondition().isA(SqlKind.LESS_THAN_OR_EQUAL)
          && PlanUtils.containsRowNumberDedup(filter);
    }
  }
}
