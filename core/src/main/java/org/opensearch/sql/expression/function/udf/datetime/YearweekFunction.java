/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.yearweekToday;

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
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class YearweekFunction extends ImplementorUDF {
  public YearweekFunction() {
    super(new YearweekImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return INTEGER_FORCE_NULLABLE;
  }

  public static class YearweekImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> newList =
          addTypeAndContext(list, rexCall, rexToLixTranslator.getRoot());
      return Expressions.call(YearweekFunction.class, "yearweek", newList);
    }
  }

  public static Object yearweek(Object date, SqlTypeName dateType, DataContext propertyContext) {
    return yearweek(date, 0, dateType, SqlTypeName.INTEGER, propertyContext);
  }

  public static Object yearweek(
      Object date,
      int mode,
      SqlTypeName dateType,
      SqlTypeName ignored,
      DataContext propertyContext) {
    FunctionProperties restored = restoreFunctionProperties(propertyContext);
    if (dateType == SqlTypeName.TIME) {
      return yearweekToday(new ExprIntegerValue(mode), restored.getQueryStartClock())
          .integerValue();
    }
    ExprValue exprValue = transferInputToExprValue(date, dateType);
    ExprValue yearWeekValue = exprYearweek(exprValue, new ExprIntegerValue(mode));
    return yearWeekValue.integerValue();
  }
}
