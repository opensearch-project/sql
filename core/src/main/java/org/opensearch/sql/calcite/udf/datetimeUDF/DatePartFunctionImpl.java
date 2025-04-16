/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

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
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class DatePartFunctionImpl extends ImplementorUDF {
  public DatePartFunctionImpl(TimeUnit timeUnit) {
    super(new DatePartImplementor(timeUnit), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  public static class DatePartImplementor implements NotNullImplementor {
    private final TimeUnit timeUnit;

    public DatePartImplementor(TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

      Expression unit = Expressions.constant(timeUnit.name());
      Expression datetime = translatedOperands.getFirst();
      SqlTypeName datetimeType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().getFirst().getType());

      return Expressions.call(
          DatePartImplementor.class,
          "date_part",
          Expressions.convert_(unit, String.class),
          Expressions.convert_(datetime, Object.class),
          Expressions.constant(datetimeType));
    }

    public static int date_part(String part, Object datetime, SqlTypeName datetimeType) {
      // TODO: restore function properties
      FunctionProperties properties = new FunctionProperties();

      ExprValue candidate =
          DateTimeApplyUtils.transferInputToExprTimestampValue(datetime, datetimeType, properties);

      if (datetimeType == SqlTypeName.TIME) {
        return DateTimeFunctions.exprExtractForTime(
                properties, new ExprStringValue(part), candidate)
            .integerValue();
      }
      return DateTimeFunctions.formatExtractFunction(new ExprStringValue(part), candidate)
          .integerValue();
    }
  }
}
