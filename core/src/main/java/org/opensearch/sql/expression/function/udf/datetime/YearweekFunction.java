/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.yearweekToday;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>yearweek(date[, mode])</code> returns the year and week for the given date as an integer
 * (e.g. 202034). The optional mode argument specifies the start of the week, where 0 means a week
 * starts on Sunday.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(DATE/TIME/TIMESTAMP/STRING) -> INTEGER
 *   <li>(STRING/DATE/TIME/TIMESTAMP, INTEGER) -> INTEGER
 * </ul>
 */
public class YearweekFunction extends ImplementorUDF {
  public YearweekFunction() {
    super(new YearweekImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.INTEGER_FORCE_NULLABLE;
  }

  public static class YearweekImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> operands = convertToExprValues(list, rexCall);
      List<Expression> operandsWithProperties =
          prependFunctionProperties(operands, rexToLixTranslator);
      return Expressions.call(YearweekFunction.class, "yearweek", operandsWithProperties);
    }
  }

  public static int yearweek(FunctionProperties properties, ExprValue date) {
    return yearweek(properties, date, ExprValueUtils.integerValue(0));
  }

  public static int yearweek(FunctionProperties properties, ExprValue date, ExprValue mode) {
    if (date.type() == ExprCoreType.TIME) {
      return yearweekToday(mode, properties.getQueryStartClock()).integerValue();
    }
    ExprValue yearWeekValue = exprYearweek(date, mode);
    return yearWeekValue.integerValue();
  }
}
