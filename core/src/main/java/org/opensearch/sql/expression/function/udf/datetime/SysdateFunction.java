/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.time.Clock;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Returns the timestamp at which it <b>executes</b>. It differs from the behavior for NOW(), which
 * returns a constant time that indicates the time at which the statement began to execute. If an
 * argument is given, it specifies a fractional seconds precision from 0 to 6, the return value
 * includes a fractional seconds part of that many digits.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>() -> TIMESTAMP
 *   <li>(INTEGER) -> TIMESTAMP
 * </ul>
 */
public class SysdateFunction extends ImplementorUDF {

  public SysdateFunction() {
    super(new SysdateImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE;
  }

  public static class SysdateImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(SysdateImplementor.class, "sysdate", translatedOperands);
    }

    public static Object sysdate() {
      var localDateTime = DateTimeFunctions.formatNow(Clock.systemDefaultZone(), 0);
      return new ExprTimestampValue(localDateTime).valueForCalcite();
    }

    public static Object sysdate(int precision) {
      var localDateTime = DateTimeFunctions.formatNow(Clock.systemDefaultZone(), precision);
      return new ExprTimestampValue(localDateTime).valueForCalcite();
    }
  }
}
