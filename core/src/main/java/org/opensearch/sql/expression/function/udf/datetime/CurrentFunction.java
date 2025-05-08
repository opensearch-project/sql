/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Implementation of the now-like functions:
 *
 * <ul>
 *   <li>Date synonyms: CURRENT_DATE, CUR_DATE
 *   <li>Time synonyms: CURTIME, CURRENT_TIME
 *   <li>Timestamp synonyms: CURRENT_TIMESTAMP, NOW, LOCALTIMESTAMP, LOCALTIME
 * </ul>
 *
 * <p>It returns the current date, time, or timestamp based on the specified return type.
 */
public class CurrentFunction extends ImplementorUDF {
  private final ExprType returnType;

  public CurrentFunction(ExprType returnType) {
    super(new CurrentFunctionImplementor(returnType), NullPolicy.NONE);
    this.returnType = returnType;
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding ->
        switch (returnType) {
          case ExprCoreType.DATE -> UserDefinedFunctionUtils.NULLABLE_DATE_UDT;
          case ExprCoreType.TIME -> UserDefinedFunctionUtils.NULLABLE_TIME_UDT;
          case ExprCoreType.TIMESTAMP -> UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
          default -> throw new IllegalArgumentException("Unsupported return type: " + returnType);
        };
  }

  @RequiredArgsConstructor
  public static class CurrentFunctionImplementor implements NotNullImplementor {
    private final ExprType returnType;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

      String functionName =
          switch (returnType) {
            case ExprCoreType.DATE -> "currentDate";
            case ExprCoreType.TIME -> "currentTime";
            case ExprCoreType.TIMESTAMP -> "currentTimestamp";
            default -> throw new IllegalArgumentException("Unsupported return type: " + returnType);
          };

      Expression properties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());
      Expression now =
          Expressions.call(CurrentFunctionImplementor.class, "getNowFromProperties", properties);

      return Expressions.call(CurrentFunctionImplementor.class, functionName, now);
    }

    public static LocalDateTime getNowFromProperties(FunctionProperties functionProperties) {
      return DateTimeFunctions.formatNow(functionProperties.getQueryStartClock());
    }

    public static Object currentDate(LocalDateTime now) {
      return new ExprDateValue(now.toLocalDate()).valueForCalcite();
    }

    public static Object currentTime(LocalDateTime now) {
      return new ExprTimeValue(now.toLocalTime()).valueForCalcite();
    }

    public static Object currentTimestamp(LocalDateTime now) {
      return new ExprTimestampValue(now).valueForCalcite();
    }
  }
}
