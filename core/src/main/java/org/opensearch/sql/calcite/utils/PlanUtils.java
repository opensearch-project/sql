/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.rex.RexWindowBounds.CURRENT_ROW;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING;
import static org.apache.calcite.rex.RexWindowBounds.following;
import static org.apache.calcite.rex.RexWindowBounds.preceding;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.WindowBound;
import org.opensearch.sql.ast.expression.WindowFrame;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public interface PlanUtils {

  String ROW_NUMBER_COLUMN_NAME = "_row_number_";
  String ROW_NUMBER_COLUMN_NAME_MAIN = "_row_number_main_";
  String ROW_NUMBER_COLUMN_NAME_SUBSEARCH = "_row_number_subsearch_";

  static SpanUnit intervalUnitToSpanUnit(IntervalUnit unit) {
    return switch (unit) {
      case MICROSECOND -> SpanUnit.MILLISECOND;
      case SECOND -> SpanUnit.SECOND;
      case MINUTE -> SpanUnit.MINUTE;
      case HOUR -> SpanUnit.HOUR;
      case DAY -> SpanUnit.DAY;
      case WEEK -> SpanUnit.WEEK;
      case MONTH -> SpanUnit.MONTH;
      case QUARTER -> SpanUnit.QUARTER;
      case YEAR -> SpanUnit.YEAR;
      case UNKNOWN -> SpanUnit.UNKNOWN;
      default -> throw new UnsupportedOperationException("Unsupported interval unit: " + unit);
    };
  }

  static RexNode makeOver(
      CalcitePlanContext context,
      BuiltinFunctionName functionName,
      RexNode field,
      List<RexNode> argList,
      List<RexNode> partitions,
      List<RexNode> orderKeys,
      @Nullable WindowFrame windowFrame) {
    if (windowFrame == null) {
      windowFrame = WindowFrame.rowsUnbounded();
    }
    boolean rows = windowFrame.getType() == WindowFrame.FrameType.ROWS;
    RexWindowBound lowerBound = convert(context, windowFrame.getLower());
    RexWindowBound upperBound = convert(context, windowFrame.getUpper());
    switch (functionName) {
        // There is no "avg" AggImplementor in Calcite, we have to change avg window
        // function to `sum over(...).toRex / count over(...).toRex`
      case AVG:
        // avg(x) ==>
        //     sum(x) / count(x)
        return context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE,
            sumOver(context, field, partitions, rows, lowerBound, upperBound),
            context.relBuilder.cast(
                countOver(context, field, partitions, rows, lowerBound, upperBound),
                SqlTypeName.DOUBLE));
        // stddev_pop(x) ==>
        //     power((sum(x * x) - sum(x) * sum(x) / count(x)) / count(x), 0.5)
        //
        // stddev_samp(x) ==>
        //     power((sum(x * x) - sum(x) * sum(x) / count(x)) / (count(x) - 1), 0.5)
        //
        // var_pop(x) ==>
        //     (sum(x * x) - sum(x) * sum(x) / count(x)) / count(x)
        //
        // var_samp(x) ==>
        //     (sum(x * x) - sum(x) * sum(x) / count(x)) / (count(x) - 1)
      case STDDEV_POP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, true, true);
      case STDDEV_SAMP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, false, true);
      case VARPOP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, true, false);
      case VARSAMP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, false, false);
      case ROW_NUMBER:
        return withOver(
            context.relBuilder.aggregateCall(SqlStdOperatorTable.ROW_NUMBER),
            partitions,
            orderKeys,
            true,
            lowerBound,
            upperBound);
      default:
        return withOver(
            makeAggCall(context, functionName, false, field, argList),
            partitions,
            orderKeys,
            rows,
            lowerBound,
            upperBound);
    }
  }

  private static RexNode sumOver(
      CalcitePlanContext ctx,
      RexNode operation,
      List<RexNode> partitions,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound) {
    return withOver(
        ctx.relBuilder.sum(operation), partitions, List.of(), rows, lowerBound, upperBound);
  }

  private static RexNode countOver(
      CalcitePlanContext ctx,
      RexNode operation,
      List<RexNode> partitions,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound) {
    return withOver(
        ctx.relBuilder.count(ImmutableList.of(operation)),
        partitions,
        List.of(),
        rows,
        lowerBound,
        upperBound);
  }

  private static RexNode withOver(
      RelBuilder.AggCall aggCall,
      List<RexNode> partitions,
      List<RexNode> orderKeys,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound) {
    return aggCall
        .over()
        .partitionBy(partitions)
        .orderBy(orderKeys)
        .let(
            c ->
                rows
                    ? c.rowsBetween(lowerBound, upperBound)
                    : c.rangeBetween(lowerBound, upperBound))
        .toRex();
  }

  private static RexNode variance(
      CalcitePlanContext ctx,
      RexNode operator,
      List<RexNode> partitions,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean biased,
      boolean sqrt) {
    RexNode argSquared = ctx.relBuilder.call(SqlStdOperatorTable.MULTIPLY, operator, operator);
    RexNode sumArgSquared = sumOver(ctx, argSquared, partitions, rows, lowerBound, upperBound);
    RexNode sum = sumOver(ctx, operator, partitions, rows, lowerBound, upperBound);
    RexNode sumSquared = ctx.relBuilder.call(SqlStdOperatorTable.MULTIPLY, sum, sum);
    RexNode count = countOver(ctx, operator, partitions, rows, lowerBound, upperBound);
    RexNode countCast = ctx.relBuilder.cast(count, SqlTypeName.DOUBLE);
    RexNode avgSumSquared = ctx.relBuilder.call(SqlStdOperatorTable.DIVIDE, sumSquared, countCast);
    RexNode diff = ctx.relBuilder.call(SqlStdOperatorTable.MINUS, sumArgSquared, avgSumSquared);
    RexNode denominator;
    if (biased) {
      denominator = countCast;
    } else {
      RexNode one = ctx.relBuilder.literal(1);
      denominator = ctx.relBuilder.call(SqlStdOperatorTable.MINUS, countCast, one);
    }
    RexNode div = ctx.relBuilder.call(SqlStdOperatorTable.DIVIDE, diff, denominator);
    RexNode result = div;
    if (sqrt) {
      RexNode half = ctx.relBuilder.literal(0.5);
      result = ctx.relBuilder.call(SqlStdOperatorTable.POWER, div, half);
    }
    return result;
  }

  static RexWindowBound convert(CalcitePlanContext context, WindowBound windowBound) {
    if (windowBound instanceof WindowBound.UnboundedWindowBound unbounded) {
      if (unbounded.isPreceding()) {
        return UNBOUNDED_PRECEDING;
      } else {
        return UNBOUNDED_FOLLOWING;
      }
    } else if (windowBound instanceof WindowBound.CurrentRowWindowBound current) {
      return CURRENT_ROW;
    } else if (windowBound instanceof WindowBound.OffSetWindowBound offset) {
      if (offset.isPreceding()) {
        return preceding(context.relBuilder.literal(offset.getOffset()));
      } else {
        return following(context.relBuilder.literal(offset.getOffset()));
      }
    } else {
      throw new UnsupportedOperationException("Unexpected window bound: " + windowBound);
    }
  }

  static RelBuilder.AggCall makeAggCall(
      CalcitePlanContext context,
      BuiltinFunctionName functionName,
      boolean distinct,
      RexNode field,
      List<RexNode> argList) {
    return PPLFuncImpTable.INSTANCE.resolveAgg(functionName, distinct, field, argList, context);
  }

  /** Get all uniq input references from a RexNode. */
  static List<RexInputRef> getInputRefs(RexNode node) {
    List<RexInputRef> inputRefs = new ArrayList<>();
    node.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            if (!inputRefs.contains(inputRef)) {
              inputRefs.add(inputRef);
            }
            return null;
          }
        });
    return inputRefs;
  }

  /** Get all uniq input references from a list of RexNodes. */
  static List<RexInputRef> getInputRefs(List<RexNode> nodes) {
    return nodes.stream().flatMap(node -> getInputRefs(node).stream()).toList();
  }

  /** Get all uniq input references from a list of agg calls. */
  static List<RexInputRef> getInputRefsFromAggCall(List<RelBuilder.AggCall> aggCalls) {
    return aggCalls.stream()
        .map(RelBuilder.AggCall::over)
        .map(RelBuilder.OverCall::toRex)
        .flatMap(rex -> getInputRefs(rex).stream())
        .toList();
  }

  /**
   * Visit the children of an unresolved plan to find it leaf relation
   *
   * @param node to visit its children
   * @return the relation if found
   */
  static UnresolvedPlan getRelation(UnresolvedPlan node) {
    AbstractNodeVisitor<Relation, Object> relationVisitor =
        new AbstractNodeVisitor<Relation, Object>() {
          @Override
          public Relation visitRelation(Relation node, Object context) {
            return node;
          }
        };
    return node.getChild().getFirst().accept(relationVisitor, null);
  }

  /**
   * Transform plan to attach specified child to the first leaf node.
   *
   * @param node to transform
   * @param child to attach
   */
  static void transformPlanToAttachChild(UnresolvedPlan node, UnresolvedPlan child) {
    AbstractNodeVisitor<Void, Object> leafVisitor =
        new AbstractNodeVisitor<Void, Object>() {
          @Override
          public Void visitChildren(Node node, Object context) {
            if (node.getChild() == null || node.getChild().isEmpty()) {
              // find leaf node
              ((UnresolvedPlan) node).attach(child);
            } else {
              node.getChild().forEach(child -> child.accept(this, context));
            }
            return null;
          }
        };
    node.accept(leafVisitor, null);
  }
}
