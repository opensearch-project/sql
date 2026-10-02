/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Planner rule that converts specific aggCall to a more efficient expressions, which includes:
 *
 * <p>- SUM(FIELD + NUMBER) -> SUM(FIELD) + NUMBER * COUNT()
 *
 * <p>- SUM(FIELD - NUMBER) -> SUM(FIELD) - NUMBER * COUNT()
 *
 * <p>- SUM(FIELD * NUMBER) -> SUM(FIELD) * NUMBER
 *
 * <p>- SUM(FIELD / NUMBER) -> SUM(FIELD) / NUMBER, Don't support this because of precision issue
 *
 * <p>TODO:
 *
 * <p>- AVG/MAX/MIN(FIELD [+|-|*|+|/] NUMBER) -> AVG/MAX/MIN(FIELD) [+|-|*|+|/] NUMBER
 */
@Value.Enclosing
public class PPLAggregateConvertRule extends RelRule<PPLAggregateConvertRule.Config>
    implements SubstitutionRule {
  /** Creates a OpenSearchAggregateConvertRule. */
  protected PPLAggregateConvertRule(Config config) {
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

    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    relBuilder.push(project.getInput());

    /*
    Build new projects with fields to be used in the converted agg call.
    Need to build this project in advance since building converted agg call has dependency on it.
    */
    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    final List<RexNode> newChildProjects = new ArrayList<>(project.getProjects());
    List<Integer> convertedAggCallArgs =
        aggCalls.stream()
            .filter(aggCall -> isConvertableAggCall(aggCall, project))
            .map(
                aggCall -> {
                  RexInputRef rexRef =
                      getFieldAndLiteral(project.getProjects().get(aggCall.getArgList().getFirst()))
                          .getLeft();
                  // Don't remove elements in the child project since we don't know if it will be
                  // used by
                  // other aggCall, will handle unused projects later
                  int ref = newChildProjects.indexOf(rexRef);
                  if (ref == -1) {
                    ref = newChildProjects.size();
                    newChildProjects.add(rexRef);
                  }
                  return ref;
                })
            .toList();
    // Stop processing if there is no converted agg call args
    if (convertedAggCallArgs.isEmpty()) return;
    relBuilder.project(newChildProjects);
    RelNode newInput = relBuilder.peek();

    /* Build converted agg call and its parent projects */
    int convertedAggCallCnt = 0;
    final int groupSetOffset = aggregate.getGroupSet().cardinality();
    final List<AggregateCall> distinctAggregateCalls = new ArrayList<>();
    final PairList<OperatorConstructor, String> newExprOnAggCall = PairList.of();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      AggregateCall aggCall = aggregate.getAggCallList().get(i);
      if (isConvertableAggCall(aggCall, project)) {
        // The arg ref of convertable aggCall starts at the end of the project
        int argRef = convertedAggCallArgs.get(convertedAggCallCnt++);
        AggregateCall sumCall =
            AggregateCall.create(
                aggCall.getParserPosition(),
                aggCall.getAggregation(),
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                aggCall.ignoreNulls(),
                aggCall.rexList,
                ImmutableList.of(argRef),
                aggCall.filterArg,
                aggCall.distinctKeys,
                aggCall.collation,
                aggregate.getGroupCount(),
                newInput, // Note: must be the new Project
                null, // The type will be inferred.
                aggCall.getName() + "_SUM");
        int sumCallRef = putToDistinctAggregateCalls(distinctAggregateCalls, sumCall);

        final Function<RelNode, Function<RexNode, RexNode>> literalConverterProvider;
        RexCall rexCall = (RexCall) project.getProjects().get(aggCall.getArgList().getFirst());
        if (rexCall.getOperator().kind == SqlKind.PLUS
            || rexCall.getOperator().kind == SqlKind.MINUS
            || rexCall.getOperator().kind == SqlKind.CHECKED_PLUS
            || rexCall.getOperator().kind == SqlKind.CHECKED_MINUS) {
          AggregateCall countCall =
              AggregateCall.create(
                  aggCall.getParserPosition(),
                  SqlStdOperatorTable.COUNT,
                  aggCall.isDistinct(),
                  aggCall.isApproximate(),
                  aggCall.ignoreNulls(),
                  aggCall.rexList,
                  ImmutableList.of(argRef),
                  aggCall.filterArg,
                  aggCall.distinctKeys,
                  aggCall.collation,
                  aggregate.getGroupCount(),
                  newInput,
                  null, // The type will be inferred.
                  aggCall.getName() + "_COUNT");
          int countCallRef = putToDistinctAggregateCalls(distinctAggregateCalls, countCall);
          literalConverterProvider =
              input ->
                  literal ->
                      rexBuilder.makeCall(
                          aggCall.getType(),
                          SqlStdOperatorTable.MULTIPLY,
                          List.of(
                              rexBuilder.makeInputRef(input, groupSetOffset + countCallRef),
                              literal));
        } else {
          literalConverterProvider = input -> literal -> literal;
        }
        newExprOnAggCall.add(
            input -> {
              Function<RexNode, RexNode> fieldConverter =
                  field -> rexBuilder.makeInputRef(input, groupSetOffset + sumCallRef);
              Function<RexNode, RexNode> literalConverter = literalConverterProvider.apply(input);
              List<RexNode> operands =
                  List.of(
                      convertToNewOperand(
                          rexCall.getOperands().getFirst(), fieldConverter, literalConverter),
                      convertToNewOperand(
                          rexCall.getOperands().getLast(), fieldConverter, literalConverter));
              return rexBuilder.makeCall(aggCall.getType(), rexCall.getOperator(), operands);
            },
            aggCall.getName());
      } else {
        int callRef = putToDistinctAggregateCalls(distinctAggregateCalls, aggCall);
        newExprOnAggCall.add(
            input -> rexBuilder.makeInputRef(input, groupSetOffset + callRef), aggCall.getName());
      }
    }

    relBuilder.aggregate(relBuilder.groupKey(aggregate.getGroupSet()), distinctAggregateCalls);

    /* Build the final project-aggregate-project */
    List<RexNode> parentProjects =
        new ArrayList<>(relBuilder.fields(IntStream.range(0, groupSetOffset).boxed().toList()));
    parentProjects.addAll(
        newExprOnAggCall.transform(
            (constructor, name) ->
                aliasMaybe(relBuilder, constructor.apply(relBuilder.peek()), name)));
    relBuilder.project(parentProjects);
    call.transformTo(relBuilder.build());
    PlanUtils.tryPruneRelNodes(call);
  }

  interface OperatorConstructor {
    RexNode apply(RelNode input);
  }

  private int putToDistinctAggregateCalls(
      List<AggregateCall> distinctAggregateCalls, AggregateCall aggCall) {
    int i = distinctAggregateCalls.indexOf(aggCall);
    if (i < 0) {
      i = distinctAggregateCalls.size();
      distinctAggregateCalls.add(aggCall);
    }
    return i;
  }

  private boolean isConvertableAggCall(AggregateCall aggCall, Project project) {
    if (aggCall.getAggregation().getKind() != SqlKind.SUM) {
      return false;
    }
    RexNode arg = project.getProjects().get(aggCall.getArgList().getFirst());
    if (!Config.isCallWithLiteral(arg)) {
      return false;
    }
    // Dropping a widening cast is only type-safe when the sum's return type doesn't depend on
    // its input type, which holds for CHECKED_LONG_SUM (always BIGINT).
    return Config.getField((RexCall) arg) instanceof RexInputRef
        || aggCall.getAggregation() == PPLBuiltinOperators.CHECKED_LONG_SUM;
  }

  private static Pair<RexInputRef, RexLiteral> getFieldAndLiteral(RexNode node) {
    RexCall call = (RexCall) node;
    RexNode arg1 = call.getOperands().getFirst();
    RexNode arg2 = call.getOperands().getLast();
    return arg1.getKind() == SqlKind.LITERAL
        ? Pair.of(Config.unwrapField(arg2), (RexLiteral) arg1)
        : Pair.of(Config.unwrapField(arg1), (RexLiteral) arg2);
  }

  private static RexNode convertToNewOperand(
      RexNode operand,
      Function<RexNode, RexNode> fieldConverter,
      Function<RexNode, RexNode> literalConverter) {
    if (operand.getKind() == SqlKind.LITERAL) {
      return literalConverter.apply(operand);
    } else {
      return fieldConverter.apply(operand);
    }
  }

  private RexNode aliasMaybe(RelBuilder builder, RexNode node, String alias) {
    return alias == null ? node : builder.alias(node, alias);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    Config SUM_CONVERTER =
        ImmutablePPLAggregateConvertRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .predicate(Config::containsSumAggCall)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .predicate(Config::containsCallWithNumber)
                                    .anyInputs()));

    static boolean containsSumAggCall(LogicalAggregate aggregate) {
      return aggregate.getAggCallList().stream()
          .anyMatch(aggCall -> aggCall.getAggregation().getKind() == SqlKind.SUM);
    }

    static boolean containsCallWithNumber(LogicalProject project) {
      return project.getProjects().stream().anyMatch(Config::isCallWithLiteral);
    }

    private static boolean isCallWithLiteral(RexNode node) {
      if (CONVERTABLE_FUNCTIONS.contains(node.getKind()) && node instanceof RexCall call) {
        RexNode arg1 = call.getOperands().getFirst();
        RexNode arg2 = call.getOperands().getLast();
        return (unwrapField(arg1) != null && arg2.getKind() == SqlKind.LITERAL)
            || (arg1.getKind() == SqlKind.LITERAL && unwrapField(arg2) != null);
      }
      return false;
    }

    /** Returns the non-literal operand of a call accepted by {@link #isCallWithLiteral}. */
    private static RexNode getField(RexCall call) {
      RexNode arg1 = call.getOperands().getFirst();
      return arg1.getKind() == SqlKind.LITERAL ? call.getOperands().getLast() : arg1;
    }

    /**
     * Returns the input ref of a bare field or of a lossless integer widening cast of one (e.g.
     * {@code CAST($0 AS BIGINT)} on a SMALLINT field, added by PPL arithmetic to prevent overflow),
     * otherwise null.
     */
    private static RexInputRef unwrapField(RexNode node) {
      if (node instanceof RexInputRef ref) {
        return ref;
      }
      if (node.getKind() == SqlKind.CAST
          && ((RexCall) node).getOperands().getFirst() instanceof RexInputRef ref) {
        int from = SqlTypeName.INT_TYPES.indexOf(ref.getType().getSqlTypeName());
        int to = SqlTypeName.INT_TYPES.indexOf(node.getType().getSqlTypeName());
        return from >= 0 && to >= from ? ref : null;
      }
      return null;
    }

    List<SqlKind> CONVERTABLE_FUNCTIONS =
        List.of(
            SqlKind.PLUS,
            SqlKind.MINUS,
            SqlKind.TIMES,
            SqlKind.CHECKED_PLUS,
            SqlKind.CHECKED_MINUS,
            SqlKind.CHECKED_TIMES
            // Don't support division because of the issue of integer division
            // e.g. (2000 / 3) * 3 = 1998 while 2000 * 3 / 3 = 2000
            // SqlKind.DIVIDE
            );

    @Override
    default PPLAggregateConvertRule toRule() {
      return new PPLAggregateConvertRule(this);
    }
  }
}
