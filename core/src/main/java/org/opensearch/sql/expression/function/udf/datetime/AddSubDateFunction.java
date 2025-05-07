/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.NULLABLE_DATE_UDT;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
import static org.opensearch.sql.utils.DateTimeUtils.extractTimestamp;

import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Adds or sub an interval or a number of days to a date or time.
 *
 * <ul>
 *   <li><code>adddate(date, INTERVAL expr unit)</code>: Adds the specified interval to the date.
 *   <li><code>adddate(date, days)</code>: Adds the specified number of days to the date.
 * </ul>
 *
 * <p>Return types:
 *
 * <ul>
 *   <li><code>(DATE/TIMESTAMP/TIME, INTERVAL) -> TIMESTAMP</code>
 *   <li><code>(DATE, LONG) -> DATE</code>
 *   <li><code>(TIMESTAMP/TIME, LONG) -> TIMESTAMP</code>
 * </ul>
 */
public class AddSubDateFunction extends ImplementorUDF {
  public AddSubDateFunction(boolean isAdd) {
    super(new AddSubDateImplementor(isAdd), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      RelDataType temporalType = opBinding.getOperandType(0);
      RelDataType temporalDeltaType = opBinding.getOperandType(1);
      if (OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType) == SqlTypeName.DATE
          && SqlTypeFamily.NUMERIC.contains(temporalDeltaType)) {
        return NULLABLE_DATE_UDT;
      } else {
        return NULLABLE_TIMESTAMP_UDT;
      }
    };
  }

  @RequiredArgsConstructor
  public static class AddSubDateImplementor implements NotNullImplementor {
    private final boolean isAdd;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression temporal = translatedOperands.get(0);
      Expression temporalDelta = translatedOperands.get(1);
      RelDataType temporalType = call.getOperands().get(0).getType();
      RelDataType temporalDeltaType = call.getOperands().get(1).getType();

      Expression base =
          Expressions.call(
              DateTimeApplyUtils.class,
              "transferInputToExprValue",
              temporal,
              Expressions.constant(
                  OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType)));

      Expression properties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());

      if (SqlTypeFamily.NUMERIC.contains(temporalDeltaType)) {
        String dateApplyFuncName =
            SqlTypeName.DATE.equals(
                    OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType))
                ? "dateApplyDaysOnDate"
                : "dateApplyDaysOnTimestamp";
        return Expressions.call(
            AddSubDateImplementor.class,
            dateApplyFuncName,
            properties,
            base,
            Expressions.convert_(temporalDelta, long.class),
            Expressions.constant(isAdd));
      } else if (SqlTypeFamily.DATETIME_INTERVAL.contains(temporalDeltaType)) {
        Expression interval =
            Expressions.call(
                DateTimeApplyUtils.class,
                "convertToTemporalAmount",
                Expressions.convert_(temporalDelta, long.class),
                Expressions.constant(
                    Objects.requireNonNull(temporalDeltaType.getIntervalQualifier()).getUnit()));

        return Expressions.call(
            AddSubDateImplementor.class,
            "dateApplyInterval",
            properties,
            base,
            interval,
            Expressions.constant(isAdd));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "The second argument of %s function must be a number or an interval",
                isAdd ? "date_add" : "date_sub"));
      }
    }

    public static String dateApplyDaysOnDate(
        FunctionProperties ignored, ExprValue datetime, long days, boolean isAdd) {
      return (String)
          new ExprDateValue(
                  isAdd
                      ? datetime.dateValue().plusDays(days)
                      : datetime.dateValue().minusDays(days))
              .valueForCalcite();
    }

    public static String dateApplyDaysOnTimestamp(
        FunctionProperties properties, ExprValue datetime, long days, boolean isAdd) {
      var dt = extractTimestamp(datetime, properties).atZone(ZoneOffset.UTC).toLocalDateTime();
      return (String)
          new ExprTimestampValue(isAdd ? dt.plusDays(days) : dt.minusDays(days)).valueForCalcite();
    }

    public static String dateApplyInterval(
        FunctionProperties properties, ExprValue datetime, TemporalAmount interval, boolean isAdd) {
      return (String)
          DateTimeFunctions.exprDateApplyInterval(properties, datetime, interval, isAdd)
              .valueForCalcite();
    }
  }
}
