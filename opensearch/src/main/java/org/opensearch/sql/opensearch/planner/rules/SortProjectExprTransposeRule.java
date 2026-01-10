/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/**
 * An optimization rule to support translating a project expression collation to its input
 * collation. Limited expressions are supported like +, -, *, CAST. With this translation, we can
 * transpose Sort - Project pattern to allow pushing down equivalent field sort into scan as if we
 * push down sort expression script into scan.
 */
@Value.Enclosing
public class SortProjectExprTransposeRule
    extends InterruptibleRelRule<SortProjectExprTransposeRule.Config> {

  protected SortProjectExprTransposeRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Project project = call.rel(1);

    if (sort.getConvention() != project.getConvention()) {
      return;
    }

    List<RelFieldCollation> pushable = new ArrayList<>();
    boolean allPushable = true;
    for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
      RexNode expr = project.getProjects().get(fieldCollation.getFieldIndex());

      Optional<Pair<Integer, Boolean>> pushableExprInputInfo =
          OpenSearchRelOptUtil.getOrderEquivalentInputInfo(expr);
      if (pushableExprInputInfo.isEmpty()) {
        allPushable = false;
        break;
      }

      int inputIndex = pushableExprInputInfo.get().getLeft();
      boolean flipped = pushableExprInputInfo.get().getRight();
      // TODO: need to determine whether we want to reverse null direction
      Direction dir =
          flipped ? fieldCollation.getDirection().reverse() : fieldCollation.getDirection();

      pushable.add(new RelFieldCollation(inputIndex, dir, fieldCollation.nullDirection));
    }
    // Not support partial collations pushdown because output needs resorting anyway
    if (!allPushable || pushable.isEmpty()) {
      return;
    }

    // Build transposed sort
    RelCollation inputCollation = RelCollations.of(pushable);
    Sort lowerSort =
        sort.copy(
            sort.getTraitSet().replace(inputCollation),
            project.getInput(),
            inputCollation,
            null,
            null);
    RelNode result;
    if (sort.fetch == null && sort.offset == null) {
      result =
          project.copy(sort.getTraitSet(), lowerSort, project.getProjects(), project.getRowType());
    } else {
      // Handle sort with limit case. We need to split the original sort into two logical sorts.
      // The higher sort is a logical limit and the lower sort ensures equivalent collations.
      Sort limitSort =
          sort.copy(
              sort.getTraitSet().replace(RelCollations.EMPTY),
              lowerSort,
              RelCollations.EMPTY,
              sort.offset,
              sort.fetch);
      result =
          project.copy(sort.getTraitSet(), limitSort, project.getProjects(), project.getRowType());
    }

    Map<RelNode, RelNode> equiv;
    if (sort.offset == null
        && sort.fetch == null
        && project
            .getCluster()
            .getPlanner()
            .getRelTraitDefs()
            .contains(RelCollationTraitDef.INSTANCE)) {
      equiv = ImmutableMap.of(lowerSort, project.getInput());
    } else {
      equiv = ImmutableMap.of();
    }

    call.transformTo(result, equiv);
  }

  /**
   * Only match logical sort and project to reduce RelSubset searching space. To support limit
   * transpose, we can only match LogicalSort because limit operator is different between logical
   * and physical conventions, aka LogicalSort with fetch vs EnumerableLimit.
   */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    SortProjectExprTransposeRule.Config DEFAULT =
        ImmutableSortProjectExprTransposeRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(Sort.class)
                        .oneInput(
                            b1 ->
                                b1.operand(Project.class)
                                    .predicate(
                                        Predicate.not(Project::containsOver)
                                            .and(PlanUtils::containsRexCall))
                                    .anyInputs()));

    @Override
    default SortProjectExprTransposeRule toRule() {
      return new SortProjectExprTransposeRule(this);
    }
  }
}
