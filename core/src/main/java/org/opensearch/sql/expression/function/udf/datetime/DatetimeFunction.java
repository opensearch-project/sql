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
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class DatetimeFunction extends ImplementorUDF {
  public DatetimeFunction() {
    super(new DatetimeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return UserDefinedFunctionUtils.TIMESTAMP_INFERENCE;
  }

  public static class DatetimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(DatetimeImplementor.class, "datetime", translatedOperands);
    }

    public static Object datetime(String timestamp) {
      ExprValue argTimestampExpr = new ExprStringValue(timestamp);
      ExprValue datetimeExpr;
      datetimeExpr = DateTimeFunctions.exprDateTimeNoTimezone(argTimestampExpr);
      return datetimeExpr.valueForCalcite();
    }

    public static Object datetime(String timestamp, String timezone) {
      ExprValue timestampExpr = new ExprStringValue(timestamp);
      ExprValue datetimeExpr =
          DateTimeFunctions.exprDateTime(timestampExpr, new ExprStringValue(timezone));
      return datetimeExpr.valueForCalcite();
    }
  }
}
