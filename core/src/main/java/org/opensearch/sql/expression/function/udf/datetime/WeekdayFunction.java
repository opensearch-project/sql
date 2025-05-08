/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprWeekday;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;

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
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>weekday(date)</code> returns the weekday index for date (0 = Monday, 1 = Tuesday, ..., 6 =
 * Sunday).
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(STRING/DATE/TIME/TIMESTAMP) -> INTEGER
 * </ul>
 */
public class WeekdayFunction extends ImplementorUDF {
  public WeekdayFunction() {
    super(new WeekdayImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.INTEGER_FORCE_NULLABLE;
  }

  public static class WeekdayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> operands) {
      Expression functionProperties =
          Expressions.call(
              UserDefinedFunctionUtils.class,
              "restoreFunctionProperties",
              rexToLixTranslator.getRoot());
      ExprType dateType =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(
              rexCall.getOperands().getFirst().getType());
      Expression date =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              operands.getFirst(),
              Expressions.constant(dateType));

      if (ExprCoreType.TIME.equals(dateType)) {
        return Expressions.call(WeekdayImplementor.class, "weekdayForTime", functionProperties);
      }
      return Expressions.call(WeekdayImplementor.class, "weekday", date);
    }

    public static int weekday(ExprValue date) {
      return exprWeekday(date).integerValue();
    }

    public static int weekdayForTime(FunctionProperties functionProperties) {
      return formatNow(functionProperties.getQueryStartClock()).getDayOfWeek().getValue() - 1;
    }
  }
}
