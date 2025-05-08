/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Returns the last day of the month of the given datetime.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(DATE/TIME/TIMESTAMP/STRING) -> DATE
 * </ul>
 */
public class LastDayFunction extends ImplementorUDF {
  public LastDayFunction() {
    super(new LastDayImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.DATE_FORCE_NULLABLE;
  }

  public static class LastDayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ExprType dateType =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(
              call.getOperands().getFirst().getType());
      Expression functionProperties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());

      if (ExprCoreType.TIME.equals(dateType)) {
        return Expressions.call(LastDayImplementor.class, "lastDayToday", functionProperties);
      }

      Expression timestampValue =
          Expressions.call(
              DateTimeApplyUtils.class,
              "transferInputToExprTimestampValue",
              translatedOperands.getFirst(),
              Expressions.constant(dateType),
              functionProperties);
      return Expressions.call(LastDayImplementor.class, "lastDay", timestampValue);
    }

    public static String lastDayToday(FunctionProperties properties) {
      return (String)
          DateTimeFunctions.exprLastDayToday(properties.getQueryStartClock()).valueForCalcite();
    }

    public static String lastDay(ExprValue timestampValue) {
      return (String) DateTimeFunctions.exprLastDay(timestampValue).valueForCalcite();
    }
  }
}
