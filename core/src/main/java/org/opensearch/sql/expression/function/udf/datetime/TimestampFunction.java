/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprTimestampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprAddTime;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class TimestampFunction extends ImplementorUDF {
  public TimestampFunction() {
    super(new TimestampImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return timestampInference;
  }

  public static class TimestampImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> enrichedList =
          addTypeAndContext(list, rexCall, rexToLixTranslator.getRoot());
      return Expressions.call(TimestampFunction.class, "timestamp", enrichedList);
    }
  }

  public static Object timestamp(
      Object datetime, SqlTypeName datetimeType, DataContext propertyContext) {
    FunctionProperties restored = restoreFunctionProperties(propertyContext);
    return transferInputToExprTimestampValue(datetime, datetimeType, restored).valueForCalcite();
  }

  public static Object timestamp(
      Object datetime,
      Object addTime,
      SqlTypeName datetimeType,
      SqlTypeName addTimeType,
      DataContext propertyContext) {
    FunctionProperties restored = restoreFunctionProperties(propertyContext);
    ExprValue dateTimeBase = transferInputToExprTimestampValue(datetime, datetimeType, restored);
    ExprValue addTimeValue = transferInputToExprTimestampValue(addTime, addTimeType, restored);
    return exprAddTime(restored, dateTimeBase, addTimeValue).valueForCalcite();
  }
}
