/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.TransferUserDefinedAggFunction;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.SpanUnit;
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
      List<RexNode> partitions) {
    if (BuiltinFunctionName.ofWindowFunction(name).isEmpty())
      throw new UnsupportedOperationException("Unexpected window function: " + name);
    BuiltinFunctionName functionName = BuiltinFunctionName.ofWindowFunction(name).get();

    switch (functionName) {
        // There is no "avg" AggImplementor in Calcite, we have to change avg window
        // function to `sum over(...).toRex / count over(...).toRex`
      case AVG:
        return context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE,
            context.relBuilder.cast(
                context.relBuilder.sum(field).over().partitionBy(partitions).toRex(),
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
            .toRex();
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
