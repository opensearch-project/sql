/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
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
      CalciteLogicalIndexScan newScan = scan.pushDownScriptProject(project);
      if (PlanUtils.containsRexLiteral(project)) {
        // literals in project list cannot be pushdown
        call.transformTo(call.builder().push(newScan).project(project.getProjects()).build());
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
                            Predicate.not(PlanUtils::containsRexOver)
                                .and(PlanUtils::containsRexCall))
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        Predicate.not(
                                                OpenSearchIndexScanRule::isScriptProjectPushed)
                                            .and(OpenSearchIndexScanRule::isProjectPushed)
                                            .and(OpenSearchIndexScanRule::noAggregatePushed))
                                    .noInputs()));

    @Override
    default OpenSearchScriptProjectIndexScanRule toRule() {
      return new OpenSearchScriptProjectIndexScanRule(this);
    }
  }
}
