/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

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
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class ExtractFunctionImpl extends ImplementorUDF {
  public ExtractFunctionImpl() {
    super(new ExtractImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BIGINT_FORCE_NULLABLE;
  }

  public static class ExtractImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      SqlTypeName datetimeType;
      Expression unit;
      Expression datetime;
      unit = translatedOperands.get(0);
      datetime = translatedOperands.get(1);
      datetimeType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().get(1).getType());

      return Expressions.call(
          ExtractImplementor.class,
          "extract",
          Expressions.convert_(unit, String.class),
          Expressions.convert_(datetime, Object.class),
          Expressions.constant(datetimeType));
    }

    public static long extract(String part, Object datetime, SqlTypeName datetimeType) {
      // TODO: restore function properties
      FunctionProperties properties = new FunctionProperties();

      ExprValue candidate = DateTimeApplyUtils.transferInputToExprValue(datetime, datetimeType);

      if (datetimeType == SqlTypeName.TIME) {
        return DateTimeFunctions.exprExtractForTime(
                properties, new ExprStringValue(part), candidate)
            .longValue();
      }
      return DateTimeFunctions.formatExtractFunction(new ExprStringValue(part), candidate)
          .longValue();
    }
  }
}
