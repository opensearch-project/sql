/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
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
      Expression unit = translatedOperands.get(0);
      Expression datetime = translatedOperands.get(1);
      ExprType datetimeType =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(call.getOperands().get(1).getType());

      Expression functionProperties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());

      Expression exprDatetimeValue =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              datetime,
              Expressions.constant(datetimeType));

      Expression part = Expressions.new_(ExprStringValue.class, unit);

      if (ExprCoreType.TIME.equals(datetimeType)) {
        return Expressions.call(
            ExtractImplementor.class,
            "extractForTime",
            functionProperties,
            part,
            exprDatetimeValue);
      }
      return Expressions.call(ExtractImplementor.class, "extract", part, exprDatetimeValue);
    }

    public static long extract(ExprStringValue part, ExprValue datetime) {
      return DateTimeFunctions.formatExtractFunction(part, datetime).longValue();
    }

    public static long extractForTime(
        FunctionProperties functionProperties, ExprStringValue part, ExprValue datetime) {
      return DateTimeFunctions.exprExtractForTime(functionProperties, part, datetime).longValue();
    }
  }
}
