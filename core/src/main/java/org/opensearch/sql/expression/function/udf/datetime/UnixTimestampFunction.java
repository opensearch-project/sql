/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Converts given argument to Unix time. If no argument given, it returns the current Unix time.
 *
 * <p>The date argument may be a DATE, or TIMESTAMP string, or a number in YYMMDD, YYMMDDhhmmss,
 * YYYYMMDD, or YYYYMMDDhhmmss format. If the argument includes a time part, it may optionally
 * include a fractional seconds part.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>() -> DOUBLE
 *   <li>(DOUBLE/LONG/DATE/TIMESTAMP) -> DOUBLE
 * </ul>
 */
public class UnixTimestampFunction extends ImplementorUDF {
  public UnixTimestampFunction() {
    super(new UnixTimestampImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.DOUBLE_FORCE_NULLABLE;
  }

  public static class UnixTimestampImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> operands = convertToExprValues(list, rexCall);
      List<Expression> operandsWithProperties =
          prependFunctionProperties(operands, rexToLixTranslator);
      return Expressions.call(UnixTimestampFunction.class, "unixTimestamp", operandsWithProperties);
    }
  }

  public static double unixTimestamp(FunctionProperties properties) {
    return unixTimeStamp(properties.getQueryStartClock()).doubleValue();
  }

  public static double unixTimestamp(FunctionProperties ignored, ExprValue timestamp) {
    return unixTimeStampOf(timestamp).doubleValue();
  }
}
