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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.udf.udaf.BrainLogPatternAggFunction;
import org.opensearch.sql.calcite.udf.udaf.PercentileApproxFunction;
import org.opensearch.sql.calcite.udf.udaf.TakeAggFunction;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public interface AggregateUtils {

  static RelBuilder.AggCall translate(
      String funcName,
      boolean distinct,
      RexNode field,
      CalcitePlanContext context,
      List<RexNode> argList) {
    if (BuiltinFunctionName.ofAggregation(funcName).isEmpty())
      throw new IllegalStateException("Unexpected value: " + funcName);

    // Additional aggregation function operators will be added here
    BuiltinFunctionName functionName = BuiltinFunctionName.ofAggregation(funcName).get();
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
      case BRAIN:
        return TransferUserDefinedAggFunction(
            BrainLogPatternAggFunction.class,
            "brain",
            ReturnTypes.explicit(SqlTypeName.ANY).andThen(SqlTypeTransforms.TO_NULLABLE),
            List.of(field),
            argList,
            context.relBuilder);
    }
    throw new IllegalStateException("Not Supported value: " + funcName);
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
