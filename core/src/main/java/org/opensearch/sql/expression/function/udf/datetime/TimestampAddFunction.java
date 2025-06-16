/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampAdd;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampAddForTimeType;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * <code>timestampadd(unit, number, datetime)</code> adds the specified number of time units to the
 * given datetime.
 *
 * <p>If the datetime is a STRING, it must be formatted as a valid TIMESTAMP. If only a TIME is
 * provided, a TIMESTAMP is still returned with the DATE portion filled in using the current date.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(STRING, INTEGER, DATE/TIME/TIMESTAMP/STRING) -> TIMESTAMP
 * </ul>
 */
public class TimestampAddFunction extends ImplementorUDF {
  public TimestampAddFunction() {
    super(new TimestampAddImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING)
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME)));
  }

  public static class TimestampAddImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ExprType timestampBaseType =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(call.getOperands().get(2).getType());
      Expression timestampBase =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              translatedOperands.get(2),
              Expressions.constant(timestampBaseType));
      if (ExprCoreType.TIME.equals(timestampBaseType)) {
        return Expressions.call(
            TimestampAddImplementor.class,
            "timestampAddForTimeType",
            translatedOperands.get(0),
            translatedOperands.get(1),
            timestampBase,
            translator.getRoot());
      } else {
        return Expressions.call(
            TimestampAddImplementor.class,
            "timestampAdd",
            translatedOperands.get(0),
            translatedOperands.get(1),
            timestampBase);
      }
    }

    public static String timestampAddForTimeType(
        String addUnit, long amount, ExprValue timestampBase, DataContext propertyContext) {
      FunctionProperties restored =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);

      return (String)
          exprTimestampAddForTimeType(
                  restored.getQueryStartClock(),
                  new ExprStringValue(addUnit),
                  new ExprLongValue(amount),
                  timestampBase)
              .valueForCalcite();
    }

    public static String timestampAdd(String addUnit, long amount, ExprValue timestampBase) {
      ExprValue returnValue =
          exprTimestampAdd(new ExprStringValue(addUnit), new ExprLongValue(amount), timestampBase);
      return (String) returnValue.valueForCalcite();
    }
  }
}
