/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprValue;
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
      SqlTypeName dateType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().getFirst().getType());
      return Expressions.call(
          LastDayImplementor.class,
          "lastDay",
          translatedOperands.getFirst(),
          Expressions.constant(dateType),
          translator.getRoot());
    }

    public static Object lastDay(String date, SqlTypeName dateType, DataContext propertyContext) {
      FunctionProperties properties =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);
      if (SqlTypeName.TIME.equals(dateType)) {
        return DateTimeFunctions.exprLastDayToday(properties.getQueryStartClock())
            .valueForCalcite();
      }
      ExprValue timestampValue =
          DateTimeApplyUtils.transferInputToExprTimestampValue(date, dateType, properties);
      return DateTimeFunctions.exprLastDay(timestampValue).valueForCalcite();
    }
  }
}
