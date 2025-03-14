/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.STDDEV_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_POP_NULLABLE;
import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.VAR_SAMP_NULLABLE;
import static org.opensearch.sql.calcite.utils.UserDefineFunctionUtils.TransferUserDefinedAggFunction;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.udf.udaf.PercentileApproxFunction;
import org.opensearch.sql.calcite.udf.udaf.TakeAggFunction;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public interface AggregateUtils {

  static RelBuilder.AggCall translate(
      AggregateFunction agg, RexNode field, CalcitePlanContext context, List<RexNode> argList) {
    if (BuiltinFunctionName.ofAggregation(agg.getFuncName()).isEmpty())
      throw new IllegalStateException("Unexpected value: " + agg.getFuncName());

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
            UserDefineFunctionUtils.getReturnTypeInferenceForArray(),
            List.of(field),
            argList,
            context.relBuilder);
      case PERCENTILE_APPROX:
        return TransferUserDefinedAggFunction(
            PercentileApproxFunction.class,
            "percentile_approx",
            UserDefineFunctionUtils.getReturnTypeInference(0),
            List.of(field),
            argList,
            context.relBuilder);
    }
    throw new IllegalStateException("Not Supported value: " + agg.getFuncName());
  }

  static AggregateCall aggCreate(SqlAggFunction agg, boolean isDistinct, RexNode field) {
    int index = ((RexInputRef) field).getIndex();
    return AggregateCall.create(
        agg,
        isDistinct,
        false,
        false,
        ImmutableList.of(),
        ImmutableList.of(index),
        -1,
        null,
        RelCollations.EMPTY,
        field.getType(),
        null);
  }
}
