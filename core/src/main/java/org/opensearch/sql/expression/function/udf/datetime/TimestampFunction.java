/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeConversionUtils.convertToTimestampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprAddTime;

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
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * It constructs a timestamp based on the input datetime value. If a second argument is provided, it
 * adds the time of the second argument to the first datetime.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(STRING/DATE/TIME/TIMESTAMP) -> TIMESTAMP
 *   <li>(STRING/DATE/TIME/TIMESTAMP, STRING/DATE/TIME/TIMESTAMP) -> TIMESTAMP
 * </ul>
 */
public class TimestampFunction extends ImplementorUDF {
  public TimestampFunction() {
    super(new TimestampImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE;
  }

  public static class TimestampImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> operands = convertToExprValues(list, rexCall);
      List<Expression> operandsWithProperties =
          prependFunctionProperties(operands, rexToLixTranslator);
      return Expressions.call(TimestampFunction.class, "timestamp", operandsWithProperties);
    }
  }

  public static Object timestamp(FunctionProperties properties, ExprValue datetime) {
    return convertToTimestampValue(datetime, properties).valueForCalcite();
  }

  public static Object timestamp(
      FunctionProperties properties, ExprValue datetime, ExprValue addTime) {
    ExprValue dateTimeBase = convertToTimestampValue(datetime, properties);
    ExprValue addTimeValue = convertToTimestampValue(addTime, properties);
    return exprAddTime(properties, dateTimeBase, addTimeValue).valueForCalcite();
  }
}
