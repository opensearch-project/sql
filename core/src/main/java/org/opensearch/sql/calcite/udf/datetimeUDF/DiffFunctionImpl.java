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
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/** Implementation for DATEDIFF and TIMESTAMPDIFF functions. */
public class DiffFunctionImpl extends ImplementorUDF {
  protected DiffFunctionImpl(NotNullImplementor implementor, NullPolicy nullPolicy) {
    super(implementor, nullPolicy);
  }

  public static DiffFunctionImpl datediff() {
    return new DiffFunctionImpl(new DiffImplementor(TimeUnit.DAY), NullPolicy.ANY);
  }

  public static DiffFunctionImpl timestampdiff() {
    // diff unit should be passed from the TIMESTAMPDIFF function call
    return new DiffFunctionImpl(new DiffImplementor(null), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BIGINT_FORCE_NULLABLE;
  }

  public static class DiffImplementor implements NotNullImplementor {
    private final TimeUnit diffUnit;

    public DiffImplementor(TimeUnit diffUnit) {
      super();
      this.diffUnit = diffUnit;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      int startIndex, endIndex;
      Expression unit;
      if (diffUnit == null) {
        // timestampdiff(interval, start, end)
        unit = translatedOperands.getFirst();
        startIndex = 1;
        endIndex = 2;
      } else {
        // datediff(end, start)
        startIndex = 1;
        endIndex = 0;
        unit = Expressions.constant(diffUnit.name());
      }
      var endType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().get(endIndex).getType());
      var startType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().get(startIndex).getType());
      return Expressions.call(
          DiffImplementor.class,
          "diff",
          unit,
          translatedOperands.get(startIndex),
          Expressions.constant(startType),
          translatedOperands.get(endIndex),
          Expressions.constant(endType));
    }

    public static Long diff(
        String unit, String start, SqlTypeName startType, String end, SqlTypeName endType) {
      // TODO: Restore function properties
      FunctionProperties restored = new FunctionProperties();
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
