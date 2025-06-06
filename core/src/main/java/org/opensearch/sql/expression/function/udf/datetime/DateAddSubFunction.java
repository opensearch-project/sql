/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeConversionUtils;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Add or sub a specified interval to a date or time. If the first argument is TIME, today's date is
 * used; if it is DATE, the time is set to midnight.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(DATE/TIMESTAMP/TIME, INTERVAL) -> TIMESTAMP
 * </ul>
 */
public class DateAddSubFunction extends ImplementorUDF {

  public DateAddSubFunction(boolean isAdd) {
    super(new DateAddSubImplementor(isAdd), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE;
  }

  @RequiredArgsConstructor
  public static class DateAddSubImplementor implements NotNullImplementor {
    private final boolean isAdd;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression temporal = translatedOperands.get(0);
      Expression temporalDelta = translatedOperands.get(1);
      RelDataType temporalType = call.getOperands().get(0).getType();
      RelDataType temporalDeltaType = call.getOperands().get(1).getType();
      Expression interval =
          Expressions.call(
              DateTimeConversionUtils.class,
              "convertToTemporalAmount",
              Expressions.convert_(temporalDelta, long.class),
              Expressions.constant(
                  Objects.requireNonNull(temporalDeltaType.getIntervalQualifier()).getUnit()));

      Expression base =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              temporal,
              Expressions.constant(
                  OpenSearchTypeFactory.convertRelDataTypeToExprType(temporalType)));

      Expression properties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());

      String funcName = isAdd ? "dateAddInterval" : "dateSubInterval";

      return Expressions.call(
          AddSubDateFunction.AddSubDateImplementor.class, funcName, properties, base, interval);
    }
  }
}
