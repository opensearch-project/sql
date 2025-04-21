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
import org.opensearch.sql.ast.expression.AggregateFunction;
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
    if (BuiltinFunctionName.ofAggregation(name).isEmpty())
      throw new IllegalArgumentException("Unexpected value: " + name);

    BuiltinFunctionName functionName = BuiltinFunctionName.ofAggregation(name).get();
    switch (functionName) {
      case MAX:
        return context.relBuilder.max(field).over().partitionBy(partitions).toRex();
      case MIN:
        return context.relBuilder.min(field).over().partitionBy(partitions).toRex();
      case AVG:
        // There is no "avg" AggImplementor in Calcite, we have to change avg window
        // function to `sum over(...) / count over(...)`
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
      case COUNT:
        return context
            .relBuilder
            .count(field == null ? ImmutableList.of() : ImmutableList.of(field))
            .over()
            .partitionBy(partitions)
            .toRex();
      case SUM:
        return context.relBuilder.sum(field).over().partitionBy(partitions).toRex();
    }
    throw new IllegalArgumentException("Not Supported window function: " + name);
  }

  static RelBuilder.AggCall makeAggCall(
      CalcitePlanContext context, AggregateFunction agg, RexNode field, List<RexNode> argList) {
    if (BuiltinFunctionName.ofAggregation(agg.getFuncName()).isEmpty())
      throw new IllegalArgumentException("Unexpected value: " + agg.getFuncName());

    // Additional aggregation function operators will be added here
    BuiltinFunctionName functionName = BuiltinFunctionName.ofAggregation(agg.getFuncName()).get();
    switch (functionName) {
      case MAX:
        return context.relBuilder.max(field);
      case MIN:
        return context.relBuilder.min(field);
      case AVG:
        return context.relBuilder.avg(agg.getDistinct(), null, field);
      case COUNT:
        return context.relBuilder.count(
            agg.getDistinct(), null, field == null ? ImmutableList.of() : ImmutableList.of(field));
      case SUM:
        return context.relBuilder.sum(agg.getDistinct(), null, field);
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
    }
    throw new IllegalStateException("Not Supported value: " + agg.getFuncName());
  }
}
