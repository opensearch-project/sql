/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.addTypeAndContext;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprTimestampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSecondsForIntType;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>to_seconds(date)</code> returns the number of seconds since the year 0 of the given
 * datetime. If the date is provided as a number, it must be formatted as YMMDD, YYMMDD, YYYMMDD or
 * YYYYMMDD.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(STRING/LONG/DATE/TIME/TIMESTAMP) -> BIGINT
 * </ul>
 */
public class ToSecondsFunction extends ImplementorUDF {
  public ToSecondsFunction() {
    super(new ToSecondsImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BIGINT_FORCE_NULLABLE;
  }

  public static class ToSecondsImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> newList = addTypeAndContext(list, rexCall, rexToLixTranslator.getRoot());
      return Expressions.call(ToSecondsFunction.class, "toSeconds", newList);
    }
  }

  public static Object toSeconds(
      Object datetime, SqlTypeName datetimeType, DataContext propertyContext) {
    FunctionProperties restored = restoreFunctionProperties(propertyContext);
    return switch (datetimeType) {
      case DATE, TIME, TIMESTAMP, CHAR, VARCHAR -> {
        ExprValue dateTimeValue =
            transferInputToExprTimestampValue(datetime, datetimeType, restored);
        yield exprToSeconds(dateTimeValue).longValue();
      }
      default -> exprToSecondsForIntType(new ExprLongValue((Number) datetime)).longValue();
    };
  }
}
