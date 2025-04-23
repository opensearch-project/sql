/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprDate;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class DateFunction extends ImplementorUDF {
  public DateFunction() {
    super(new DateImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return dateInference;
  }

  public static class DateImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> newList =
          addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
      return Expressions.call(
          Types.lookupMethod(DateFunction.class, "eval", Object[].class), newList);
    }
  }

  public static Object eval(Object... args) {
    FunctionProperties restored = restoreFunctionProperties((DataContext) args[args.length - 1]);
    ExprValue candidate = transferInputToExprValue(args[0], (SqlTypeName) args[1]);
    if ((SqlTypeName) args[1] == SqlTypeName.TIME) {
      return new ExprDateValue(((ExprTimeValue) candidate).dateValue(restored)).valueForCalcite();
    }
    return exprDate(candidate).valueForCalcite();
  }
}
