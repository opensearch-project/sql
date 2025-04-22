/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampAdd;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampAddForTimeType;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class TimestampAddFunction extends ImplementorUDF {
  public TimestampAddFunction() {
    super(new TimestampAddImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> UserDefinedFunctionUtils.nullableTimestampUDT;
  }

  public static class TimestampAddImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      SqlTypeName timestampBaseType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().get(2).getType());
      Expression timestampBase =
          Expressions.call(
              DateTimeApplyUtils.class,
              "transferInputToExprValue",
              translatedOperands.get(2),
              Expressions.constant(timestampBaseType));
      if (timestampBaseType.equals(SqlTypeName.TIME)) {
        return Expressions.call(
            TimestampAddImplementor.class,
            "timestampAddForTimeType",
            translatedOperands.get(0),
            translatedOperands.get(1),
            timestampBase,
            Expressions.convert_(translator.getRoot(), Object.class));
      } else {
        return Expressions.call(
            TimestampAddImplementor.class,
            "timestampAdd",
            translatedOperands.get(0),
            translatedOperands.get(1),
            timestampBase);
      }
    }

    public static Object timestampAddForTimeType(
        String addUnit, long amount, ExprValue timestampBase, Object propertyContext) {
      FunctionProperties restored =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);

      return exprTimestampAddForTimeType(
              restored.getQueryStartClock(),
              new ExprStringValue(addUnit),
              new ExprLongValue(amount),
              timestampBase)
          .valueForCalcite();
    }

    public static Object timestampAdd(String addUnit, long amount, ExprValue timestampBase) {
      ExprValue returnValue =
          exprTimestampAdd(new ExprStringValue(addUnit), new ExprLongValue(amount), timestampBase);
      return returnValue.valueForCalcite();
    }
  }
}
