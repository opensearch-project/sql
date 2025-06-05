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
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>sec_to_time(number)</code> converts the number of seconds to time in HH:mm:ssss[.nnnnnn]
 * format.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(INTEGER/LONG/DOUBLE/FLOAT) -> TIME
 * </ul>
 */
public class SecToTimeFunction extends ImplementorUDF {
  public SecToTimeFunction() {
    super(new SecToTimeImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.TIME_FORCE_NULLABLE;
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

    public static String secToTime(Number seconds) {
      ExprValue returnTimeValue;
      ExprValue transferredValue = ExprValueUtils.fromObjectValue(seconds);
      if (MathUtils.isIntegral(seconds)) {
        returnTimeValue = exprSecToTime(transferredValue);
      } else {
        returnTimeValue = exprSecToTimeWithNanos(transferredValue);
      }
      return (String) returnTimeValue.valueForCalcite();
    }
  }
}
