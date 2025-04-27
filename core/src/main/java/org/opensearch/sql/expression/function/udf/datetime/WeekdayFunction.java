/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprWeekday;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;

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
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
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
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> newList = addTypeAndContext(list, rexCall, rexToLixTranslator.getRoot());
      return Expressions.call(WeekdayFunction.class, "weekday", newList);
    }
  }

  public static Object weekday(Object date, SqlTypeName dateType, DataContext propertyContext) {
    FunctionProperties restored = restoreFunctionProperties(propertyContext);
    if (dateType == SqlTypeName.TIME) {
      // PPL Weekday returns 0 ~ 6; java.time.DayOfWeek returns 1 ~ 7.
      return formatNow(restored.getQueryStartClock()).getDayOfWeek().getValue() - 1;
    } else {
      return exprWeekday(transferInputToExprValue(date, dateType)).integerValue();
    }
  }
}
