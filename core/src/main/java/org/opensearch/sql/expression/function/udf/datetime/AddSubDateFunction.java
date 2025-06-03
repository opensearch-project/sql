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
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeConversionUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
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
      if (OpenSearchTypeFactory.convertRelDataTypeToExprType(temporalType) == ExprCoreType.DATE
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
              ExprValueUtils.class,
              "fromObjectValue",
              temporal,
              Expressions.constant(
                  OpenSearchTypeFactory.convertRelDataTypeToExprType(temporalType)));

      Expression properties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());

      if (SqlTypeFamily.NUMERIC.contains(temporalDeltaType)) {
        String applyDaysFuncName;
        if (ExprCoreType.DATE.equals(
            OpenSearchTypeFactory.convertRelDataTypeToExprType(temporalType))) {
          applyDaysFuncName = isAdd ? "dateAddDaysOnDate" : "dateSubDaysOnDate";
        } else {
          applyDaysFuncName = isAdd ? "dateAddDaysOnTimestamp" : "dateSubDaysOnTimestamp";
        }
        return Expressions.call(
            AddSubDateImplementor.class,
            applyDaysFuncName,
            properties,
            base,
            Expressions.convert_(temporalDelta, long.class));
      } else if (SqlTypeFamily.DATETIME_INTERVAL.contains(temporalDeltaType)) {
        Expression interval =
            Expressions.call(
                DateTimeConversionUtils.class,
                "convertToTemporalAmount",
                Expressions.convert_(temporalDelta, long.class),
                Expressions.constant(
                    Objects.requireNonNull(temporalDeltaType.getIntervalQualifier()).getUnit()));

        String applyIntervalFuncName = isAdd ? "dateAddInterval" : "dateSubInterval";
        return Expressions.call(
            AddSubDateImplementor.class, applyIntervalFuncName, properties, base, interval);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "The second argument of %s function must be a number or an interval",
                isAdd ? "date_add" : "date_sub"));
      }
    }

    public static String dateAddDaysOnDate(
        FunctionProperties ignored, ExprValue datetime, long days) {
      return (String) new ExprDateValue(datetime.dateValue().plusDays(days)).valueForCalcite();
    }

    public static String dateSubDaysOnDate(
        FunctionProperties ignored, ExprValue datetime, long days) {
      return (String) new ExprDateValue(datetime.dateValue().minusDays(days)).valueForCalcite();
    }

    public static String dateAddDaysOnTimestamp(
        FunctionProperties properties, ExprValue datetime, long days) {
      var dt = extractTimestamp(datetime, properties).atZone(ZoneOffset.UTC).toLocalDateTime();
      return (String) new ExprTimestampValue(dt.plusDays(days).toInstant(ZoneOffset.UTC)).valueForCalcite();
    }

    public static String dateSubDaysOnTimestamp(
        FunctionProperties properties, ExprValue datetime, long days) {
      var dt = extractTimestamp(datetime, properties).atZone(ZoneOffset.UTC).toLocalDateTime();
      return (String) new ExprTimestampValue(dt.minusDays(days).toInstant(ZoneOffset.UTC)).valueForCalcite();
    }

    public static String dateAddInterval(
        FunctionProperties properties, ExprValue datetime, TemporalAmount interval) {
      var dt = extractTimestamp(datetime, properties).atZone(ZoneOffset.UTC).toLocalDateTime();
      return (String) new ExprTimestampValue(dt.plus(interval).toInstant(ZoneOffset.UTC)).valueForCalcite();
    }

    public static String dateSubInterval(
        FunctionProperties properties, ExprValue datetime, TemporalAmount interval) {
      var dt = extractTimestamp(datetime, properties).atZone(ZoneOffset.UTC).toLocalDateTime();
      return (String) new ExprTimestampValue(dt.minus(interval).toInstant(ZoneOffset.UTC)).valueForCalcite();
    }
  }
}
