/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.LogicalDedup;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
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
    final LogicalDedup logicalDedup = call.rel(0);
    final LogicalProject projectWithExpr = call.rel(1);
    final CalciteLogicalIndexScan scan = call.rel(2);
    apply(call, logicalDedup, projectWithExpr, scan);
  }

  protected void apply(
      RelOptRuleCall call,
      LogicalDedup dedup,
      LogicalProject project,
      CalciteLogicalIndexScan scan) {

    List<RexNode> dedupColumns = dedup.getDedupeFields();
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

    RelBuilder relBuilder = call.builder();
    relBuilder.push(project);

    List<RexNode> targetProjections = new ArrayList<>();
    Set<Integer> dedupFieldsIndexSet = new HashSet<>();
    for (RexNode dedupColumn : dedupColumns) {
      if (dedupColumn instanceof RexInputRef ref) {
        targetProjections.add(dedupColumn);
        dedupFieldsIndexSet.add(ref.getIndex());
      } else {
        LOG.warn("The dedup column {} is illegal.", dedupColumn);
        return;
      }
    }
    IntStream.range(0, project.getProjects().size())
        .boxed()
        .filter(index -> !dedupFieldsIndexSet.contains(index))
        .map(relBuilder::field)
        .forEach(targetProjections::add);

    relBuilder.project(targetProjections);
    LogicalProject targetChildProject = (LogicalProject) relBuilder.peek();

    if (targetChildProject.getNamedProjects().stream()
        .limit(dedupColumns.size())
        .anyMatch(
            pair ->
                Utils.resolveNestedPath(pair.getValue(), scan.getOsIndex().getFieldTypes())
                    != null)) {
      // fallback to non-pushdown if the dedup columns contain nested fields.
      return;
    }

    // 2 Push an Aggregate
    // We push down a LITERAL_AGG with dedupNumer for converting the dedup command to aggregate:
    // (1) Pass the dedupNumer to AggregateAnalyzer.processAggregateCalls()
    // (2) Distinguish it from an optimization operator and user defined aggregator.
    // (LITERAL_AGG is used in optimization normally, see {@link SqlKind#LITERAL_AGG})
    List<Integer> newGroupByList = IntStream.range(0, dedupColumns.size()).boxed().toList();
    relBuilder.aggregate(
        relBuilder.groupKey(relBuilder.fields(newGroupByList)),
        relBuilder.literalAgg(dedup.getAllowedDuplication()));

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
      call.transformTo(newScan.copyWithNewSchema(dedup.getRowType()));
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    // +- LogicalDedup
    //    +- LogicalProject
    //       +- CalciteLogicalIndexScan
    Config DEFAULT =
        ImmutableDedupPushdownRule.Config.builder()
            .build()
            .withDescription("Dedup-to-Aggregate")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalDedup.class)
                        // Cannot push dedup operator if keepEmpty=true
                        .predicate(dedup -> !dedup.getKeepEmpty())
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(CalciteLogicalIndexScan.class)
                                                .predicate(Config::tableScanChecker)
                                                .noInputs())));

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
  }
}
