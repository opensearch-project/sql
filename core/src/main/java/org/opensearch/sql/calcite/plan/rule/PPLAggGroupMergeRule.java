/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.CalciteUtils;
import org.opensearch.sql.calcite.utils.PlanUtils;

/**
 * Planner rule that merge multiple agg group fields into a single one, on which all other group
 * fields depend. e.g.
 *
 * <p>stats ... by a, f1(a), f2(a) -> stats ... by a | eval `f1(a)` = f1(a), `f2(a)` = f2(a)
 *
 * <p>TODO: this rule could be expanded further for more cases: 1. support multiple base group
 * fields, e.g. stats ... by a, f1(a), b, f2(b), f3(a, b) -> stats ... by a, b | eval `f1(a)` =
 * f1(a), `f2(b)` = f2(b), `f3(a, b)` = f3(a, b) 2. support no base fields, e.g. stats ... by f1(a),
 * f2(a) -> stats ... by a | eval `f1(a)` = f1(a), `f2(a)` = f2(a) | fields - a Note that one of
 * these UDFs' output must have equivalent cardinality as `a`.
 */
@Value.Enclosing
public class PPLAggGroupMergeRule extends RelRule<PPLAggGroupMergeRule.Config>
    implements SubstitutionRule {

  /** Creates a OpenSearchAggregateConvertRule. */
  protected PPLAggGroupMergeRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      final LogicalAggregate aggregate = call.rel(0);
      final LogicalProject project = call.rel(1);
      apply(call, aggregate, project);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  public void apply(RelOptRuleCall call, LogicalAggregate aggregate, LogicalProject project) {
    List<Integer> groupSet = aggregate.getGroupSet().asList();
    List<RexNode> groupNodes =
        groupSet.stream().map(group -> project.getProjects().get(group)).collect(Collectors.toList());
    Pair<List<Integer>, List<Integer>> baseFieldsAndOthers =
        CalciteUtils.partition(
            groupSet, i -> project.getProjects().get(i).getKind() == SqlKind.INPUT_REF);
    List<Integer> baseGroupList = baseFieldsAndOthers.getLeft();
    // TODO: support more base fields in the future.
    if (baseGroupList.size() != 1) return;
    Integer baseGroupField = baseGroupList.get(0);
    RexInputRef baseGroupRef = (RexInputRef) project.getProjects().get(baseGroupField);
    List<Integer> otherGroupList = baseFieldsAndOthers.getRight();
    boolean allDependOnBaseField =
        otherGroupList.stream()
            .map(i -> project.getProjects().get(i))
            .allMatch(node -> isDependentField(node, List.of(baseGroupRef)));
    if (!allDependOnBaseField) return;

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(project);

    relBuilder.aggregate(
        relBuilder.groupKey(ImmutableBitSet.of(baseGroupField)), aggregate.getAggCallList());

    /* Build the final project-aggregate-project */
    final Mapping mapping =
        Mappings.target(
            List.of(baseGroupRef.getIndex()),
            baseGroupRef.getIndex() + 1); // set source count greater than the max ref index
    List<RexNode> parentProjections = new ArrayList<>(RexUtil.apply(mapping, groupNodes));
    List<RexNode> aggCallRefs =
        relBuilder.fields(
            IntStream.range(baseGroupList.size(), relBuilder.peek().getRowType().getFieldCount())
                .boxed()
                .collect(Collectors.toList()));
    parentProjections.addAll(aggCallRefs);
    relBuilder.project(parentProjections);
    call.transformTo(relBuilder.build());
    PlanUtils.tryPruneRelNodes(call);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    Config GROUP_MERGE =
        ImmutablePPLAggGroupMergeRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .predicate(Config::containsMultipleGroupSets)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .predicate(Config::containsDependentFields)
                                    .anyInputs()));

    static boolean containsMultipleGroupSets(LogicalAggregate aggregate) {
      return aggregate.getGroupSet().cardinality() > 1;
    }

    // Only rough predication here since we don't know which fields are group fields currently.
    static boolean containsDependentFields(LogicalProject project) {
      Set<RexNode> baseFields =
          project.getProjects().stream()
              .filter(node -> node.getKind() == SqlKind.INPUT_REF)
              .collect(Collectors.toUnmodifiableSet());
      return project.getProjects().stream()
          .anyMatch(node -> PPLAggGroupMergeRule.isDependentField(node, baseFields));
    }

    @Override
    default PPLAggGroupMergeRule toRule() {
      return new PPLAggGroupMergeRule(this);
    }
  }

  public static boolean isDependentField(RexNode node, Collection<RexNode> baseFields) {
    // Always view literal field as dependent field here since we can always implement a function
    // to transform a field into such a literal
    if (node.getKind() == SqlKind.LITERAL) return true;
    if (node.getKind() == SqlKind.INPUT_REF && baseFields.contains(node)) return true;
    // Use !isAggregator to rule out window functions like row_number()
    if (node instanceof RexCall
        && ((RexCall) node).getOperator().isDeterministic()
        && !((RexCall) node).getOperator().isAggregator()) {
      return ((RexCall) node)
          .getOperands().stream().allMatch(op -> isDependentField(op, baseFields));
    }
    return false;
  }
}
