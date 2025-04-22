/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.nullableTimestampUDT;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprFromUnixTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprFromUnixTimeFormat;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class FromUnixTimeFunction extends ImplementorUDF {
  public FromUnixTimeFunction() {
    super(new FromUnixTimeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      if (opBinding.collectOperandTypes().size() == 1) {
        return nullableTimestampUDT;
      }
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
    };
  }

  public static class FromUnixTimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(FromUnixTimeImplementor.class, "fromUnixTime", translatedOperands);
    }

    public static Object fromUnixTime(double unixTime) {
      return exprFromUnixTime(new ExprDoubleValue(unixTime)).valueForCalcite();
    }

    public static Object fromUnixTime(double unixTime, String format) {
      return exprFromUnixTimeFormat(new ExprDoubleValue(unixTime), new ExprStringValue(format))
          .valueForCalcite();
    }
  }
}
