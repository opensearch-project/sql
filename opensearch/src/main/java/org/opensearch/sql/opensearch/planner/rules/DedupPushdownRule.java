/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import com.google.common.collect.Streams;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
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
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@Value.Enclosing
public class DedupPushdownRule extends InterruptibleRelRule<DedupPushdownRule.Config> {
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
    if (call.rels.length == 5) {
      final CalciteLogicalIndexScan scan = call.rel(4);
      apply(call, finalProject, numOfDedupFilter, projectWithWindow, null, scan);
    } else if (call.rels.length == 6) {
      final LogicalProject projectWithExpr = call.rel(4);
      final CalciteLogicalIndexScan scan = call.rel(5);
      apply(call, finalProject, numOfDedupFilter, projectWithWindow, projectWithExpr, scan);
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
      @Nullable LogicalProject projectWithExpr,
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
    List<Integer> dedupColumnIndices = getInputRefIndices(dedupColumns);
    List<String> dedupColumnNames =
        dedupColumnIndices.stream()
            .map(
                i ->
                    projectWithWindow.getNamedProjects().stream()
                        .filter(pair -> pair.getKey().isA(SqlKind.INPUT_REF))
                        .filter(pair -> ((RexInputRef) pair.getKey()).getIndex() == i)
                        .map(Pair::getValue)
                        .findFirst()
                        .get())
            .collect(Collectors.toList());
    if (dedupColumnIndices.size() != dedupColumnNames.size()) {
      return;
    }
    // must be row_number <= number
    assert numOfDedupFilter.getCondition().isA(SqlKind.LESS_THAN_OR_EQUAL);
    RexLiteral literal =
        (RexLiteral) ((RexCall) numOfDedupFilter.getCondition()).getOperands().get(1);
    Integer dedupNumer = literal.getValueAs(Integer.class);

    // We convert the dedup pushdown to composite aggregate + top_hits:
    // Aggregate(literalAgg(dedupNumer), groups)
    // +- Project(groups, remaining)
    RelBuilder relBuilder = call.builder();
    // 1 Initial a RelBuilder by pushing Scan and Project
    if (projectWithExpr == null) {
      // 1.1 if projectWithExpr not existed, push a scan then create a new project
      relBuilder.push(scan);
      List<RexNode> columnsFromScan = relBuilder.fields();
      List<String> colNamesFromScan = relBuilder.peek().getRowType().getFieldNames();
      List<Pair<RexNode, String>> namedPairFromScan =
          Streams.zip(columnsFromScan.stream(), colNamesFromScan.stream(), Pair::new).collect(Collectors.toList());
      List<Pair<RexNode, String>> reordered =
          advanceDedupColumns(namedPairFromScan, dedupColumnIndices, dedupColumnNames);
      relBuilder.project(
          reordered.stream().map(Pair::getKey).collect(Collectors.toList()),
          reordered.stream().map(Pair::getValue).collect(Collectors.toList()),
          true);
    } else {
      // 1.2 if projectWithExpr existed, push a reordered projectWithExpr
      List<Pair<RexNode, String>> reordered =
          advanceDedupColumns(
              projectWithExpr.getNamedProjects(), dedupColumnIndices, dedupColumnNames);
      LogicalProject reorderedProject =
          LogicalProject.create(
              projectWithExpr.getInput(),
              List.of(),
              reordered.stream().map(Pair::getKey).collect(Collectors.toList()),
              reordered.stream().map(Pair::getValue).collect(Collectors.toList()),
              Set.of());
      relBuilder.push(reorderedProject);
    }
    LogicalProject targetChildProject = (LogicalProject) relBuilder.peek();

    // 2 Push an Aggregate
    // We push down a LITERAL_AGG with dedupNumer for converting the dedup command to aggregate:
    // (1) Pass the dedupNumer to AggregateAnalyzer.processAggregateCalls()
    // (2) Distinguish it from an optimization operator and user defined aggregator.
    // (LITERAL_AGG is used in optimization normally, see {@link SqlKind#LITERAL_AGG})
    relBuilder.aggregate(
        relBuilder.groupKey(relBuilder.fields(dedupColumnNames)),
        relBuilder.literalAgg(dedupNumer));
    // add bucket_nullable = false hint
    PlanUtils.addIgnoreNullBucketHintToAggregate(relBuilder);
    // peek the aggregate after hint being added
    LogicalAggregate aggregate = (LogicalAggregate) relBuilder.build();

    CalciteLogicalIndexScan newScan =
        (CalciteLogicalIndexScan) scan.pushDownAggregate(aggregate, targetChildProject);
    if (newScan != null) {
      // Back to original project order
      call.transformTo(newScan.copyWithNewSchema(finalProject.getRowType()));
    }
  }

  private static List<Integer> getInputRefIndices(List<RexNode> columns) {
    return columns.stream()
        .filter(rex -> rex.isA(SqlKind.INPUT_REF))
        .map(r -> ((RexInputRef) r).getIndex())
        .collect(Collectors.toList());
  }

  /**
   * Move the dedup columns to the front of the original column list.
   *
   * @param originalColumnList The original column pair list
   * @param dedupColumnIndices The indices of dedup columns
   * @param dedupColumnNames The names of dedup columns
   * @return The reordered column pair list
   */
  private static List<Pair<RexNode, String>> advanceDedupColumns(
      List<Pair<RexNode, String>> originalColumnList,
      List<Integer> dedupColumnIndices,
      List<String> dedupColumnNames) {
    List<Pair<RexNode, String>> reordered =
        IntStream.range(0, originalColumnList.size())
            .boxed()
            .sorted(
                (i1, i2) -> {
                  boolean in1 = dedupColumnIndices.contains(i1);
                  boolean in2 = dedupColumnIndices.contains(i2);
                  return in1 == in2 ? i1 - i2 : in2 ? 1 : -1;
                })
            .map(
                i -> {
                  Pair<RexNode, String> original = originalColumnList.get(i);
                  if (dedupColumnIndices.contains(i)) {
                    int dedupIndex = dedupColumnIndices.indexOf(i);
                    return Pair.of(original.getKey(), dedupColumnNames.get(dedupIndex));
                  }
                  return original;
                })
            .collect(Collectors.toList());
    return reordered;
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    // Can only push the case with KEEPEMPTY=false:
    // +- LogicalProject(no _row_number_dedup_)
    //    +- LogicalFilter(condition contains _row_number_dedup_)
    //       +- LogicalProject(contains _row_number_dedup_)
    //          +- LogicalFilter(condition=IS NOT NULL(dedupColumn))"
    //             +- CalciteLogicalIndexScan
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
                                                        b3.operand(LogicalFilter.class)
                                                            .predicate(
                                                                PlanUtils
                                                                    ::mayBeFilterFromBucketNonNull)
                                                            .oneInput(
                                                                b4 ->
                                                                    b4.operand(
                                                                            CalciteLogicalIndexScan
                                                                                .class)
                                                                        .predicate(
                                                                            Config
                                                                                ::tableScanChecker)
                                                                        .noInputs())))));
    // +- LogicalProject(no _row_number_dedup_)
    //    +- LogicalFilter(condition contains _row_number_dedup_)
    //       +- LogicalProject(contains _row_number_dedup_)
    //          +- LogicalFilter(condition IS NOT NULL(dedupColumn))
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
                                                            .predicate(
                                                                PlanUtils
                                                                    ::mayBeFilterFromBucketNonNull)
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
          .and(Predicate.not(AbstractCalciteIndexScan::isProjectPushed))
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
