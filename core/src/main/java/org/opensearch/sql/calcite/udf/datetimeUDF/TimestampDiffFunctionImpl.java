/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampDiff;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimestampDiffForTimeType;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/** Implementation for TIMESTAMPDIFF functions. */
public class TimestampDiffFunctionImpl extends ImplementorUDF {
  public TimestampDiffFunctionImpl() {
    super(new DiffImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BIGINT_FORCE_NULLABLE;
  }

  public static class DiffImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      int startIndex, endIndex;
      Expression unit;
      // timestampdiff(interval, start, end)
      unit = translatedOperands.getFirst();
      startIndex = 1;
      endIndex = 2;
      var startType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().get(startIndex).getType());
      var endType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().get(endIndex).getType());

      return Expressions.call(
          DiffImplementor.class,
          "diff",
          unit,
          translatedOperands.get(startIndex),
          Expressions.constant(startType),
          translatedOperands.get(endIndex),
          Expressions.constant(endType),
          Expressions.convert_(translator.getRoot(), Object.class));
    }

    public static Long diff(
        String unit,
        String start,
        SqlTypeName startType,
        String end,
        SqlTypeName endType,
        Object propertyContext) {
      FunctionProperties restored =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);
      ExprValue startTimestamp = transferInputToExprValue(start, startType);
      ExprValue endTimestamp = transferInputToExprValue(end, endType);

      if (startType == SqlTypeName.TIME || endType == SqlTypeName.TIME) {
        return exprTimestampDiffForTimeType(
                restored, new ExprStringValue(unit), startTimestamp, endTimestamp)
            .longValue();
      }

      ExprValue diffResult =
          exprTimestampDiff(new ExprStringValue(unit), startTimestamp, endTimestamp);
      return diffResult.longValue();
    }
  }
}
