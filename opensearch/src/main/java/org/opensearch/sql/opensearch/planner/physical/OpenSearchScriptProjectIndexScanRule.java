/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Planner rule that push RexCalls down to {@link CalciteLogicalIndexScan} */
@Value.Enclosing
public class OpenSearchScriptProjectIndexScanRule
    extends RelRule<OpenSearchScriptProjectIndexScanRule.Config> {
  private static final Logger LOG =
      LogManager.getLogger(OpenSearchScriptProjectIndexScanRule.class);

  /** Creates a OpenSearchProjectIndexScanRule. */
  protected OpenSearchScriptProjectIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      // the ordinary variant
      final LogicalProject project = call.rel(0);
      final CalciteLogicalIndexScan scan = call.rel(1);
      apply(call, project, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(RelOptRuleCall call, LogicalProject project, CalciteLogicalIndexScan scan) {
    try {
      // literals in project list cannot be pushdown
      // TODO: Support partial project pushdown
      if (PlanUtils.containsRexLiteral(project)) {
        return;
      }
      List<String> callNames =
          project.getNamedProjects().stream()
              .filter(pair -> pair.left instanceof RexCall)
              .map(pair -> pair.getValue())
              .toList();
      CalciteLogicalIndexScan newScan = scan.pushDownScriptProject(project);
      List<String> uniquifiedNames = newScan.getPushDownContext().getDerivedFieldNames();
      Map<String, String> uniquifiedNamesMap =
          IntStream.range(0, callNames.size())
              .boxed()
              .map(i -> Pair.of(callNames.get(i), uniquifiedNames.get(i)))
              .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

      if (!callNames.equals(newScan.getPushDownContext().getDerivedFieldNames())) {
        RelBuilder relBuilder = call.builder().push(newScan);
        List<RexNode> projectNodes =
            project.getNamedProjects().stream()
                .map(
                    pair ->
                        uniquifiedNamesMap.get(pair.getValue()) == null
                            ? relBuilder.field(pair.getValue())
                            : relBuilder.field(uniquifiedNamesMap.get(pair.getValue())))
                .collect(Collectors.toList());
        call.transformTo(
            relBuilder.project(projectNodes, project.getRowType().getFieldNames()).build());
      } else {
        call.transformTo(newScan);
      }
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the script project.", e);
      } else {
        LOG.info("Cannot pushdown the script project.");
      }
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    /** Config that matches Project on OpenSearchScriptProjectIndexScanRule. */
    Config DEFAULT =
        ImmutableOpenSearchScriptProjectIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .predicate(
                            Predicate.not(LogicalProject::containsOver)
                                .and(PlanUtils::containsRexCall))
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        Predicate.not(
                                                OpenSearchIndexScanRule::isScriptProjectPushed)
                                            .and(OpenSearchIndexScanRule::isProjectPushed)
                                            .and(OpenSearchIndexScanRule::noLimitPushed)
                                            .and(OpenSearchIndexScanRule::noAggregatePushed))
                                    .noInputs()));

    @Override
    default OpenSearchScriptProjectIndexScanRule toRule() {
      return new OpenSearchScriptProjectIndexScanRule(this);
    }
  }
}
