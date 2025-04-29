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
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.TransferUserDefinedAggFunction;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.WindowBound;
import org.opensearch.sql.ast.expression.WindowFrame;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.udf.udaf.PercentileApproxFunction;
import org.opensearch.sql.calcite.udf.udaf.TakeAggFunction;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public interface PlanUtils {

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
      String name,
      RexNode field,
      List<RexNode> argList,
      List<RexNode> partitions,
      @Nullable WindowFrame windowFrame) {
    if (BuiltinFunctionName.ofWindowFunction(name).isEmpty())
      throw new UnsupportedOperationException("Unexpected window function: " + name);
    BuiltinFunctionName functionName = BuiltinFunctionName.ofWindowFunction(name).get();
    if (windowFrame == null) {
      windowFrame = WindowFrame.defaultFrame();
    }
    boolean rows = windowFrame.getType() == WindowFrame.FrameType.ROWS;
    RexWindowBound lowerBound = convert(context, windowFrame.getLower());
    RexWindowBound upperBound = convert(context, windowFrame.getUpper());
    switch (functionName) {
        // There is no "avg" AggImplementor in Calcite, we have to change avg window
        // function to `sum over(...).toRex / count over(...).toRex`
      case AVG:
        return context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE,
            context.relBuilder.cast(
                context
                    .relBuilder
                    .sum(field)
                    .over()
                    .partitionBy(partitions)
                    .let(
                        c ->
                            rows
                                ? c.rowsBetween(lowerBound, upperBound)
                                : c.rangeBetween(lowerBound, upperBound))
                    .toRex(),
                SqlTypeName.DOUBLE),
            context
                .relBuilder
                .count(ImmutableList.of(field))
                .over()
                .partitionBy(partitions)
                .toRex());
      default:
        return makeAggCall(context, name, false, field, argList)
            .over()
            .partitionBy(partitions)
            .let(
                c ->
                    rows
                        ? c.rowsBetween(lowerBound, upperBound)
                        : c.rangeBetween(lowerBound, upperBound))
            .toRex();
    }
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
      String name,
      boolean distinct,
      RexNode field,
      List<RexNode> argList) {
    if (BuiltinFunctionName.ofAggregation(name).isEmpty())
      throw new UnsupportedOperationException("Unexpected aggregation: " + name);
    BuiltinFunctionName functionName = BuiltinFunctionName.ofAggregation(name).get();

    switch (functionName) {
      case MAX:
        return context.relBuilder.max(field);
      case MIN:
        return context.relBuilder.min(field);
      case AVG:
        return context.relBuilder.avg(distinct, null, field);
      case COUNT:
        return context.relBuilder.count(
            distinct, null, field == null ? ImmutableList.of() : ImmutableList.of(field));
      case SUM:
        return context.relBuilder.sum(distinct, null, field);
        //            case MEAN:
        //                throw new UnsupportedOperationException("MEAN is not supported in PPL");
        //            case STDDEV:
        //                return context.relBuilder.aggregateCall(SqlStdOperatorTable.STDDEV,
        // field);
      case VARSAMP:
        return context.relBuilder.aggregateCall(VAR_SAMP_NULLABLE, field);
      case VARPOP:
        return context.relBuilder.aggregateCall(VAR_POP_NULLABLE, field);
      case STDDEV_POP:
        return context.relBuilder.aggregateCall(STDDEV_POP_NULLABLE, field);
      case STDDEV_SAMP:
        return context.relBuilder.aggregateCall(STDDEV_SAMP_NULLABLE, field);
        //            case PERCENTILE_APPROX:
        //                return
        // context.relBuilder.aggregateCall(SqlStdOperatorTable.PERCENTILE_CONT, field);
      case TAKE:
        return TransferUserDefinedAggFunction(
            TakeAggFunction.class,
            "TAKE",
            UserDefinedFunctionUtils.getReturnTypeInferenceForArray(),
            List.of(field),
            argList,
            context.relBuilder);
      case PERCENTILE_APPROX:
        List<RexNode> newArgList = new ArrayList<>(argList);
        newArgList.add(context.rexBuilder.makeFlag(field.getType().getSqlTypeName()));
        return TransferUserDefinedAggFunction(
            PercentileApproxFunction.class,
            "percentile_approx",
            ReturnTypes.ARG0_FORCE_NULLABLE,
            List.of(field),
            newArgList,
            context.relBuilder);
      default:
        throw new UnsupportedOperationException("Unexpected aggregation: " + name);
    }
  }
}
