/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.convertToExprValues;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.prependFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeConversionUtils.convertToTimestampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSecondsForIntType;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

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

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.DATETIME.or(OperandTypes.STRING).or(OperandTypes.INTEGER));
  }

  public static class ToSecondsImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> operands = convertToExprValues(list, rexCall);
      List<Expression> operandsWithProperties =
          prependFunctionProperties(operands, rexToLixTranslator);
      return Expressions.call(ToSecondsFunction.class, "toSeconds", operandsWithProperties);
    }
  }

  public static long toSeconds(FunctionProperties properties, ExprValue datetime) {
    return switch (datetime.type()) {
      case ExprCoreType.DATE, ExprCoreType.TIME, ExprCoreType.TIMESTAMP, ExprCoreType.STRING -> {
        ExprValue dateTimeValue = convertToTimestampValue(datetime, properties);
        yield exprToSeconds(dateTimeValue).longValue();
      }
      default -> exprToSecondsForIntType(new ExprLongValue(datetime.longValue())).longValue();
    };
  }
}
