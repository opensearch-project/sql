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
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class SysdateFunction extends ImplementorUDF {

  public SysdateFunction() {
    super(new SysdateImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return UserDefinedFunctionUtils.TIMESTAMP_INFERENCE;
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
