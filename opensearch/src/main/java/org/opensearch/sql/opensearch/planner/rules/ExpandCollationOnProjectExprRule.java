/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import static org.opensearch.sql.calcite.utils.PlanUtils.tryPruneRelNodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/**
 * When ENUMERABLE convention physical node is converted from logical node, each enumerable node's
 * collation is recalculated based on input collations. However, if SortProjectExprTransposeRule
 * takes effect, the input collation is changed to a sort over field instead of original sort over
 * expression. It changes the collation requirement of the whole query.
 *
 * <p>Another problem is if sort expression is pushed down to scan, the Enumerable project doesn't
 * know the collation is already satisfied.
 *
 * <p>AbstractConverter physical node is supposed to resolve the problem of inconsistent collation
 * requirement between physical node input and output. This optimization rule finds equivalent
 * output expression collations and input field collations. If their collation traits are satisfied,
 * generate a new RelSubset without top sort
 */
@Value.Enclosing
public class ExpandCollationOnProjectExprRule
    extends InterruptibleRelRule<ExpandCollationOnProjectExprRule.Config> {

  protected ExpandCollationOnProjectExprRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final AbstractConverter converter = call.rel(0);
    final Project project = call.rel(1);
    final RelTraitSet toTraits = converter.getTraitSet();
    final RelCollation toCollation = toTraits.getTrait(RelCollationTraitDef.INSTANCE);

    assert toCollation != null && toCollation.getFieldCollations() != null
        : "Output field collations should not be null";

    Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap = new HashMap<>();
    for (RelFieldCollation relFieldCollation : toCollation.getFieldCollations()) {
      orderEquivInfoMap.put(
          relFieldCollation.getFieldIndex(),
          OpenSearchRelOptUtil.getOrderEquivalentInputInfo(
              project.getProjects().get(relFieldCollation.getFieldIndex())));
    }

    // Branch 1: Check if complex expressions are already sorted by scan and assign collation
    if (handleComplexExpressionsSortedByScan(
        call, project, toTraits, toCollation, orderEquivInfoMap)) {
      return;
    }

    // Branch 2: Handle simple expressions that can be transformed to field sorts
    handleSimpleExpressionFieldSorts(call, project, toTraits, toCollation, orderEquivInfoMap);
  }

  /**
   * Handle the case where complex expressions are already sorted by the scan. In this case, we can
   * directly assign toTrait to the new EnumerableProject.
   *
   * @return true if handled, false if not applicable
   */
  private boolean handleComplexExpressionsSortedByScan(
      RelOptRuleCall call,
      Project project,
      RelTraitSet toTraits,
      RelCollation toCollation,
      Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap) {

    // Check if toCollation is null or not a simple RelCollation with field collations
    if (toCollation == null || toCollation.getFieldCollations().isEmpty()) {
      return false;
    }

    // Extract the actual enumerable scan from the input, handling RelSubset case
    CalciteEnumerableIndexScan scan = extractEnumerableScanFromInput(project.getInput());
    if (scan == null) {
      return false;
    }

    // Check if the scan can provide the required sort collation
    if (OpenSearchRelOptUtil.canScanProvideSortCollation(
        scan, project, toCollation, orderEquivInfoMap)) {
      // The scan has already provided the sorting for complex expressions
      // We can directly assign toTrait to new EnumerableProject
      Project newProject =
          project.copy(toTraits, project.getInput(), project.getProjects(), project.getRowType());
      call.transformTo(newProject);
      return true;
    }
    return false;
  }

  /**
   * Handle simple expressions that can be transformed to field sorts using
   * getOrderEquivalentInputInfo.
   */
  private void handleSimpleExpressionFieldSorts(
      RelOptRuleCall call,
      Project project,
      RelTraitSet toTraits,
      RelCollation toCollation,
      Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap) {

    RelTrait fromTrait = project.getInput().getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    // In case of fromTrait is an instance of RelCompositeTrait, it most likely finds equivalence by
    // default.
    // Let it go through default ExpandConversionRule to determine trait satisfaction.
    if (fromTrait instanceof RelCollation) {
      RelCollation fromCollation = (RelCollation) fromTrait;
      // TODO: Handle the case where multi expr collations are mapped to the same source field
      if (toCollation == null
          || toCollation.getFieldCollations().isEmpty()
          || fromCollation.getFieldCollations().size() < toCollation.getFieldCollations().size()) {
        return;
      }

      for (int i = 0; i < toCollation.getFieldCollations().size(); i++) {
        RelFieldCollation toCollationFieldCollation = toCollation.getFieldCollations().get(i);
        if (!OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            fromCollation.getFieldCollations().get(i),
            toCollationFieldCollation,
            orderEquivInfoMap.get(toCollationFieldCollation.getFieldIndex()))) {
          return;
        }
      }

      // After collation equivalence analysis, fromTrait satisfies toTrait. Copy the target trait
      // set to new EnumerableProject.
      Project newProject =
          project.copy(toTraits, project.getInput(), project.getProjects(), project.getRowType());
      call.transformTo(newProject);
      tryPruneRelNodes(call);
    }
  }

  /**
   * Extract CalciteEnumerableIndexScan from the input RelNode, handling RelSubset case. Since this
   * rule matches EnumerableProject, we expect CalciteEnumerableIndexScan during physical
   * optimization.
   *
   * @param input The input RelNode to extract scan from
   * @return CalciteEnumerableIndexScan if found, null otherwise
   */
  private static CalciteEnumerableIndexScan extractEnumerableScanFromInput(RelNode input) {

    // Case 1: Direct CalciteEnumerableIndexScan (physical scan)
    if (input instanceof CalciteEnumerableIndexScan) {
      return (CalciteEnumerableIndexScan) input;
    }

    // Case 2: RelSubset with best plan being a CalciteEnumerableIndexScan
    if (input instanceof RelSubset) {
      RelSubset subset = (RelSubset) input;
      RelNode bestPlan = subset.getBest();
      if (bestPlan != null) {
        // Recursively check the best plan
        return extractEnumerableScanFromInput(bestPlan);
      }
    }

    return null;
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {

    /**
     * Only match ENUMERABLE convention RelNode combination like below to narrow the optimization
     * searching space: - AbstractConverter - EnumerableProject
     */
    ExpandCollationOnProjectExprRule.Config DEFAULT =
        ImmutableExpandCollationOnProjectExprRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(AbstractConverter.class)
                        .oneInput(
                            b1 ->
                                b1.operand(EnumerableProject.class)
                                    .predicate(
                                        Predicate.not(Project::containsOver)
                                            .and(PlanUtils::containsRexCall))
                                    .anyInputs()));

    @Override
    default ExpandCollationOnProjectExprRule toRule() {
      return new ExpandCollationOnProjectExprRule(this);
    }
  }
}
