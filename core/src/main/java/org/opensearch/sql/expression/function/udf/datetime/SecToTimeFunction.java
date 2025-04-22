/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSecToTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSecToTimeWithNanos;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class SecToTimeFunction extends ImplementorUDF {
  public SecToTimeFunction() {
    super(new SecToTimeImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> UserDefinedFunctionUtils.nullableTimeUDT;
  }

  public static class SecToTimeImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          SecToTimeImplementor.class,
          "secToTime",
          Expressions.convert_(translatedOperands.getFirst(), Number.class));
    }

    public static Object secToTime(Number seconds) {
      ExprValue returnTimeValue;
      ExprValue transferredValue = ExprValueUtils.fromObjectValue(seconds);
      if (seconds instanceof Long || seconds instanceof Integer) {
        returnTimeValue = exprSecToTime(transferredValue);
      } else {
        returnTimeValue = exprSecToTimeWithNanos(transferredValue);
      }
      return returnTimeValue.valueForCalcite();
    }
  }
}
