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
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

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

  /** Discriminates the temporal flavour at registration time, decoupled from {@code ExprType}. */
  public enum Kind {
    DATE,
    TIME,
    TIMESTAMP
  }

  private final Kind kind;

  public CurrentFunction(Kind kind) {
    super(new CurrentFunctionImplementor(kind), NullPolicy.NONE);
    this.kind = kind;
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding ->
        switch (kind) {
          case DATE -> UserDefinedFunctionUtils.NULLABLE_DATE_UDT;
          case TIME -> UserDefinedFunctionUtils.NULLABLE_TIME_UDT;
          case TIMESTAMP -> UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
        };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NONE;
  }

  @RequiredArgsConstructor
  public static class CurrentFunctionImplementor implements NotNullImplementor {
    private final Kind kind;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

      String functionName =
          switch (kind) {
            case DATE -> "currentDate";
            case TIME -> "currentTime";
            case TIMESTAMP -> "currentTimestamp";
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

    public static String currentDate(LocalDateTime now) {
      return (String) new ExprDateValue(now.toLocalDate()).valueForCalcite();
    }

    public static String currentTime(LocalDateTime now) {
      return (String) new ExprTimeValue(now.toLocalTime()).valueForCalcite();
    }

    public static String currentTimestamp(LocalDateTime now) {
      return (String) new ExprTimestampValue(now).valueForCalcite();
    }
  }
}
