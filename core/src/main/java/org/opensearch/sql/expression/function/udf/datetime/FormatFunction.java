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
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

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

  /** Discriminates DATE_FORMAT (false) from TIME_FORMAT (true) at registration time. */
  public FormatFunction(boolean isTimeFormat) {
    super(new DataFormatImplementor(isTimeFormat), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.DATETIME_OR_STRING_STRING;
  }

  @RequiredArgsConstructor
  public static class DataFormatImplementor implements NotNullImplementor {
    private final boolean isTimeFormat;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      org.apache.calcite.rel.type.RelDataType firstOperandType =
          call.getOperands().getFirst().getType();
      Expression functionProperties =
          Expressions.call(
              UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());
      Expression datetime =
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              translatedOperands.get(0),
              Expressions.constant(
                  OpenSearchTypeFactory.convertRelDataTypeToExprType(firstOperandType)));
      Expression format = Expressions.new_(ExprStringValue.class, translatedOperands.get(1));

      if (isTimeFormat) {
        return Expressions.call(DataFormatImplementor.class, "timeFormat", datetime, format);
      }
      if (OpenSearchTypeFactory.isTimeExprType(firstOperandType)) {
        return Expressions.call(
            DataFormatImplementor.class, "dateFormatForTime", functionProperties, format, datetime);
      }
      return Expressions.call(DataFormatImplementor.class, "dateFormat", datetime, format);
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
