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
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class MinuteOfDayFunctionImpl extends ImplementorUDF {
  public MinuteOfDayFunctionImpl() {
    super(new MinuteOfDayImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  public static class MinuteOfDayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      SqlTypeName datetimeType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().getFirst().getType());
      return Expressions.call(
          MinuteOfDayImplementor.class,
          "minuteOfDay",
          Expressions.convert_(translatedOperands.getFirst(), Object.class),
          Expressions.constant(datetimeType));
    }

    public static int minuteOfDay(Object datetime, SqlTypeName datetimeType) {
      ExprValue candidate =
          ExprValueUtils.fromObjectValue(
              datetime, OpenSearchTypeFactory.convertSqlTypeNameToExprType(datetimeType));
      return DateTimeFunctions.exprMinuteOfDay(candidate).integerValue();
    }
  }
}
