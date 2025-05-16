/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampDiff;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampDiffForTimeType;

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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Implementation for TIMESTAMPDIFF functions.
 *
 * <p>TIMESTAMPDIFF(interval, start, end) returns the difference between the start and end
 * date/times in interval units. If a TIME is provided as an argument, it will be converted to a
 * TIMESTAMP with the DATE portion filled in using the current date.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(STRING, DATE/TIME/TIMESTAMP/STRING, DATE/TIME/TIMESTAMP/STRING) -> LONG
 * </ul>
 */
public class TimestampDiffFunction extends ImplementorUDF {
  public TimestampDiffFunction() {
    super(new DiffImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BIGINT_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(
                    SqlTypeFamily.STRING, SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME)
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.DATETIME))
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.STRING, SqlTypeFamily.DATETIME, SqlTypeFamily.STRING))
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)));
  }

  public static class DiffImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      // timestampdiff(interval, start, end)
      int startIndex = 1;
      int endIndex = 2;
      var startType =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(
              call.getOperands().get(startIndex).getType());
      var endType =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(
              call.getOperands().get(endIndex).getType());

      Expression functionProperties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());
      Expression unit = Expressions.new_(ExprStringValue.class, translatedOperands.getFirst());
      Expression start =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              translatedOperands.get(startIndex),
              Expressions.constant(startType));
      Expression end =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              translatedOperands.get(endIndex),
              Expressions.constant(endType));

      if (ExprCoreType.TIME.equals(startType) || ExprCoreType.TIME.equals(endType)) {
        return Expressions.call(
            DiffImplementor.class, "diffForTime", functionProperties, unit, start, end);
      }
      return Expressions.call(DiffImplementor.class, "diff", unit, start, end);
    }

    public static long diff(ExprStringValue unit, ExprValue start, ExprValue end) {
      ExprValue diffResult = exprTimestampDiff(unit, start, end);
      return diffResult.longValue();
    }

    public static long diffForTime(
        FunctionProperties functionProperties,
        ExprStringValue unit,
        ExprValue start,
        ExprValue end) {
      return exprTimestampDiffForTimeType(functionProperties, unit, start, end).longValue();
    }
  }
}
