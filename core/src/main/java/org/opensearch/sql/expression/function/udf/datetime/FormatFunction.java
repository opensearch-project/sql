/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedDate;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedDateOfToday;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedTime;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Implementation for date_format and time_format functions.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>date_format(DATE/TIME/TIMESTAMP/STRING, STRING) -> STRING
 *   <li>time_format(TIME/TIMESTAMP/STRING, STRING) -> STRING
 * </ul>
 */
public class FormatFunction extends ImplementorUDF {
  public FormatFunction(ExprType functionType) {
    super(new DataFormatImplementor(functionType), NullPolicy.ANY);
    if (!functionType.equals(ExprCoreType.DATE) && !functionType.equals(ExprCoreType.TIME)) {
      throw new IllegalArgumentException(
          "Function type can only be DATE or TIME, but got: " + functionType);
    }
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  @RequiredArgsConstructor
  public static class DataFormatImplementor implements NotNullImplementor {
    private final ExprType functionType;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ExprType type =
          OpenSearchTypeFactory.convertRelDataTypeToExprType(
              call.getOperands().getFirst().getType());
      Expression functionProperties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());
      Expression datetime =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              translatedOperands.get(0),
              Expressions.constant(type));
      Expression format = Expressions.new_(ExprStringValue.class, translatedOperands.get(1));

      if (ExprCoreType.TIME.equals(functionType)) {
        return Expressions.call(DataFormatImplementor.class, "timeFormat", datetime, format);
      } else {
        if (ExprCoreType.TIME.equals(type)) {
          return Expressions.call(
              DataFormatImplementor.class,
              "dateFormatForTime",
              functionProperties,
              format,
              datetime);
        } else {
          return Expressions.call(DataFormatImplementor.class, "dateFormat", datetime, format);
        }
      }
    }

    public static String dateFormat(ExprValue date, ExprStringValue format) {
      return getFormattedDate(date, format).stringValue();
    }

    public static String dateFormatForTime(
        FunctionProperties functionProperties, ExprStringValue format, ExprValue time) {
      return getFormattedDateOfToday(format, time, functionProperties.getQueryStartClock())
          .stringValue();
    }

    public static String timeFormat(ExprValue time, ExprStringValue format) {
      return getFormattedTime(time, format).stringValue();
    }
  }
}
