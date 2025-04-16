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
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class ConvertTZFunctionImpl extends ImplementorUDF {
  public ConvertTZFunctionImpl() {
    super(new ConvertTZImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> UserDefinedFunctionUtils.nullableTimestampUDT;
  }

  public static class ConvertTZImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(ConvertTZImplementor.class, "convertTz", translatedOperands);
    }

    public static Object convertTz(String timestamp, String fromTz, String toTz) {

      ExprValue datetimeExpr =
          DateTimeFunctions.exprConvertTZ(
              new ExprStringValue(timestamp),
              new ExprStringValue(fromTz),
              new ExprStringValue(toTz));

      return datetimeExpr.valueForCalcite();
    }
  }
}
