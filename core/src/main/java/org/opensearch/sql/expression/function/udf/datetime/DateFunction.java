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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>date(expr)</code> constructs a date type with the input string expr as a date. If the
 * argument is of date/timestamp, it extracts the date value part from the expression.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>STRING/DATE/TIMESTAMP -> DATE
 * </ul>
 */
public class DateFunction extends ImplementorUDF {
  public DateFunction() {
    super(new DateImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return DATE_INFERENCE;
  }

  public static class DateImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> newList = addTypeAndContext(list, rexCall, rexToLixTranslator.getRoot());
      return Expressions.call(DateFunction.class, "date", newList);
    }
  }

  public static Object date(Object date, SqlTypeName dateType, DataContext propertyContext) {
    FunctionProperties restored = restoreFunctionProperties(propertyContext);
    ExprValue dateValue = transferInputToExprValue(date, dateType);
    if (dateType == SqlTypeName.TIME) {
      return new ExprDateValue(((ExprTimeValue) dateValue).dateValue(restored)).valueForCalcite();
    }
    return exprDate(dateValue).valueForCalcite();
  }
}
