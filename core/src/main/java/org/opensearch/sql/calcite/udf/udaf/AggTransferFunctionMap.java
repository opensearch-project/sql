/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.opensearch.sql.calcite.utils.CalciteToolsHelper.*;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.TransferUserDefinedAggFunction;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.*;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class AggTransferFunctionMap {
  public static Map<BuiltinFunctionName, AggHandler> AGG_FUNCTION_MAP;

  static {
    AGG_FUNCTION_MAP = new HashMap<>();
    AGG_FUNCTION_MAP.put(MAX, (distinct, field, argList, ctx) -> ctx.relBuilder.max(field));

    AGG_FUNCTION_MAP.put(MIN, (distinct, field, argList, ctx) -> ctx.relBuilder.min(field));

    AGG_FUNCTION_MAP.put(
        AVG, (distinct, field, argList, ctx) -> ctx.relBuilder.avg(distinct, null, field));

    AGG_FUNCTION_MAP.put(
        COUNT,
        (distinct, field, argList, ctx) ->
            ctx.relBuilder.count(
                distinct, null, field == null ? ImmutableList.of() : ImmutableList.of(field)));

    AGG_FUNCTION_MAP.put(
        SUM, (distinct, field, argList, ctx) -> ctx.relBuilder.sum(distinct, null, field));

    AGG_FUNCTION_MAP.put(
        VARSAMP,
        (distinct, field, argList, ctx) -> ctx.relBuilder.aggregateCall(VAR_SAMP_NULLABLE, field));

    AGG_FUNCTION_MAP.put(
        VARPOP,
        (distinct, field, argList, ctx) -> ctx.relBuilder.aggregateCall(VAR_POP_NULLABLE, field));

    AGG_FUNCTION_MAP.put(
        STDDEV_SAMP,
        (distinct, field, argList, ctx) ->
            ctx.relBuilder.aggregateCall(STDDEV_SAMP_NULLABLE, field));

    AGG_FUNCTION_MAP.put(
        STDDEV_POP,
        (distinct, field, argList, ctx) ->
            ctx.relBuilder.aggregateCall(STDDEV_POP_NULLABLE, field));

    AGG_FUNCTION_MAP.put(
        TAKE,
        (distinct, field, argList, ctx) ->
            TransferUserDefinedAggFunction(
                TakeAggFunction.class,
                "TAKE",
                UserDefinedFunctionUtils.getReturnTypeInferenceForArray(),
                List.of(field),
                argList,
                ctx.relBuilder));

    AGG_FUNCTION_MAP.put(
        PERCENTILE_APPROX,
        (distinct, field, argList, ctx) -> {
          List<RexNode> newArgList = new ArrayList<>(argList);
          newArgList.add(ctx.rexBuilder.makeFlag(field.getType().getSqlTypeName()));
          return TransferUserDefinedAggFunction(
              PercentileApproxFunction.class,
              "percentile_approx",
              ReturnTypes.ARG0_FORCE_NULLABLE,
              List.of(field),
              newArgList,
              ctx.relBuilder);
        });
  }
}
