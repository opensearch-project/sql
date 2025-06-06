/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.INTEGER_FORCE_NULLABLE;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>week(date[, mode])</code> returns the week number for date. Mode means the start of the
 * day. 0 means a week starts at Sunday. If the mode argument is omitted, the default mode 0 is
 * used.
 *
 * <p>It differs from <code>SqlStdOperatorTable.WEEK</code> in that it supports an optional mode
 * argument.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(DATE/TIME/TIMESTAMP/STRING) -> INTEGER
 *   <li>(DATE/TIME/TIMESTAMP/STRING, INTEGER) -> INTEGER
 * </ul>
 */
public class WeekFunction extends ImplementorUDF {
  public WeekFunction() {
    super(new WeekImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return INTEGER_FORCE_NULLABLE;
  }

  public static class WeekImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      return Expressions.call(WeekImplementor.class, "week", list);
    }

    public static int week(String date) {
      ExprValue dateValue = new ExprDateValue(date);
      return (int) DateTimeFunctions.exprWeekWithoutMode(dateValue).valueForCalcite();
    }

    public static int week(String date, int mode) {
      ExprValue dateValue = new ExprDateValue(date);
      ExprValue modeValue = new ExprIntegerValue(mode);
      ExprValue woyExpr = DateTimeFunctions.exprWeek(dateValue, modeValue);
      return (int) woyExpr.valueForCalcite();
    }
  }
}
