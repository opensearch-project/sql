/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
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

import static org.opensearch.sql.data.type.ExprCoreType.*;

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
      return opBinding -> {
          RelDataType result;
          if (returnType.equals(DATE)) {
              result = UserDefinedFunctionUtils.NULLABLE_DATE_UDT;
          } else if (returnType.equals(TIME)) {
              result = UserDefinedFunctionUtils.NULLABLE_TIME_UDT;
          } else if (returnType.equals(TIMESTAMP)) {
              result = UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
          } else {
              throw new IllegalArgumentException("Unsupported return type: " + returnType);
          }
          return result;
      };
  }

  @RequiredArgsConstructor
  public static class CurrentFunctionImplementor implements NotNullImplementor {
    private final ExprType returnType;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

        String functionName;
        if (returnType.equals(DATE)) {
            functionName = "currentDate";
        } else if (returnType.equals(TIME)) {
            functionName = "currentTime";
        } else if (returnType.equals(TIMESTAMP)) {
            functionName = "currentTimestamp";
        } else {
            throw new IllegalArgumentException("Unsupported return type: " + returnType);
        }


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

    public static String currentDate(LocalDateTime now) {
      return (String) new ExprDateValue(now.toLocalDate()).valueForCalcite();
    }

    public static String currentTime(LocalDateTime now) {
      return (String) new ExprTimeValue(now.toLocalTime()).valueForCalcite();
    }

    public static String currentTimestamp(LocalDateTime now) {
      return (String) new ExprTimestampValue(now.toInstant(ZoneOffset.UTC)).valueForCalcite();
    }
  }
}
