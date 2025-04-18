/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class StrToDateFunctionImpl extends ImplementorUDF {
  public StrToDateFunctionImpl() {
    super(new StrToDateImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> UserDefinedFunctionUtils.nullableTimestampUDT;
  }

  public static class StrToDateImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

      return Expressions.call(StrToDateImplementor.class, "strToDate", translatedOperands);
    }

    public static Object strToDate(String date, String format) {
      // TODO: Restore function properties
      FunctionProperties properties = new FunctionProperties();
      return DateTimeFunctions.exprStrToDate(
              properties,
              ExprValueUtils.fromObjectValue(date),
              ExprValueUtils.fromObjectValue(format))
          .valueForCalcite();
    }
  }
}
