/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.nullableDateUDT;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.nullableTimestampUDT;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.convertToTemporalAmount;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class DateAddSubFunction extends ImplementorUDF {
  // DATE_ADD and DATE_SUB always return TIMESTAMP
  // while ADDDATE and SUBDATE return DATE if the first argument is DATE
  private final boolean alwaysReturnTimestamp;

  public DateAddSubFunction(boolean isAdd, boolean alwaysReturnTimestamp) {
    super(new DateAddSubImplementor(isAdd, alwaysReturnTimestamp), NullPolicy.ANY);
    this.alwaysReturnTimestamp = alwaysReturnTimestamp;
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding ->
        DateAddSubImplementor.deriveReturnType(
            opBinding.getOperandType(0), opBinding.getOperandType(1), alwaysReturnTimestamp);
  }

  public static class DateAddSubImplementor implements NotNullImplementor {
    private final boolean isAdd;
    private final boolean alwaysReturnTimestamp;

    public DateAddSubImplementor(boolean isAdd, boolean alwaysReturnTimestamp) {
      this.isAdd = isAdd;
      this.alwaysReturnTimestamp = alwaysReturnTimestamp;
    }

    public static RelDataType deriveReturnType(
        RelDataType temporalType, RelDataType temporalDeltaType, boolean alwaysReturnTimestamp) {
      if (!alwaysReturnTimestamp
          && OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType) == SqlTypeName.DATE
          && SqlTypeFamily.NUMERIC.contains(temporalDeltaType)) {
        return nullableDateUDT;
      } else {
        return nullableTimestampUDT;
      }
    }

    /**
     * Implements a call with assumption that all the null-checking is implemented by caller.
     *
     * @param translator translator to implement the code
     * @param call call to implement
     * @param translatedOperands arguments of a call
     * @return expression that implements given call
     */
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression temporal = translatedOperands.get(0);
      Expression temporalDelta = translatedOperands.get(1);
      RelDataType temporalType = call.getOperands().get(0).getType();
      RelDataType temporalDeltaType = call.getOperands().get(1).getType();
      SqlTypeName returnType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              deriveReturnType(temporalType, temporalDeltaType, alwaysReturnTimestamp));

      if (SqlTypeFamily.NUMERIC.contains(temporalDeltaType)) {
        return Expressions.call(
            DateAddSubImplementor.class,
            "applyInterval",
            Expressions.convert_(temporal, Object.class),
            Expressions.constant(
                OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType)),
            Expressions.convert_(temporalDelta, long.class),
            Expressions.constant(TimeUnit.DAY),
            Expressions.constant(returnType),
            Expressions.constant(isAdd),
            Expressions.convert_(translator.getRoot(), Object.class));
      } else if (SqlTypeFamily.DATETIME_INTERVAL.contains(temporalDeltaType)) {
        return Expressions.call(
            DateAddSubImplementor.class,
            "applyInterval",
            Expressions.convert_(temporal, Object.class),
            Expressions.constant(
                OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(temporalType)),
            Expressions.convert_(temporalDelta, long.class),
            Expressions.constant(
                Objects.requireNonNull(temporalDeltaType.getIntervalQualifier()).getUnit()),
            Expressions.constant(returnType),
            Expressions.constant(isAdd),
            Expressions.convert_(translator.getRoot(), Object.class));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "The second argument of %s function must be a number or an interval",
                isAdd ? "date_add" : "date_sub"));
      }
    }

    public static Object applyInterval(
        Object temporal,
        SqlTypeName temporalTypeName,
        long temporalDelta,
        TimeUnit unit,
        SqlTypeName returnSqlType,
        boolean isAdd,
        Object propertyContext) {
      ExprValue base = transferInputToExprValue(temporal, temporalTypeName);
      FunctionProperties restored =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);
      ExprValue resultDatetime =
          DateTimeFunctions.exprDateApplyInterval(
              restored, base, convertToTemporalAmount(temporalDelta, unit), isAdd);
      if (returnSqlType == SqlTypeName.TIMESTAMP) {
        return resultDatetime.valueForCalcite();
      } else {
        return new ExprDateValue(resultDatetime.dateValue()).valueForCalcite();
      }
    }
  }
}
