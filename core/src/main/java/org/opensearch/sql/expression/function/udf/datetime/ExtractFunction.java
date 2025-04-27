/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.util.List;
import org.apache.calcite.DataContext;
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
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>extract(part FROM date)</code> returns a LONG with digits in order according to the given
 * 'part' arguments.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(STRING, DATE/TIME/TIMESTAMP/STRING) -> LONG
 * </ul>
 */
public class ExtractFunction extends ImplementorUDF {
  public ExtractFunction() {
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
          Expressions.constant(datetimeType),
          translator.getRoot());
    }

    public static long extract(
        String part, Object datetime, SqlTypeName datetimeType, DataContext propertyContext) {
      FunctionProperties properties =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);

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
