/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedDate;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedDateOfToday;
import static org.opensearch.sql.expression.datetime.DateTimeFormatterUtil.getFormattedTime;

import java.util.List;
import lombok.RequiredArgsConstructor;
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
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
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
  public FormatFunction(SqlTypeName functionType) {
    super(new DataFormatImplementor(functionType), NullPolicy.ANY);
    if (!functionType.equals(SqlTypeName.DATE) && !functionType.equals(SqlTypeName.TIME)) {
      throw new IllegalArgumentException(
          "Function type can only be DATE or TIME, but got: " + functionType);
    }
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  @RequiredArgsConstructor
  public static class DataFormatImplementor implements NotNullImplementor {
    private final SqlTypeName functionType;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      SqlTypeName type =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().getFirst().getType());
      String methodName = functionType == SqlTypeName.TIME ? "timeFormat" : "dateFormat";
      return Expressions.call(
          DataFormatImplementor.class,
          methodName,
          translatedOperands.get(0),
          Expressions.constant(type),
          translatedOperands.get(1),
          translator.getRoot());
    }

    public static String dateFormat(
        Object date, SqlTypeName type, String format, DataContext propertyContext) {
      FunctionProperties restored =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);
      ExprValue candidateValue = transferInputToExprValue(date, type);
      if (type == SqlTypeName.TIME) {
        return getFormattedDateOfToday(
                new ExprStringValue(format), candidateValue, restored.getQueryStartClock())
            .stringValue();
      }
      return getFormattedDate(candidateValue, new ExprStringValue(format)).stringValue();
    }

    public static String timeFormat(
        Object time, SqlTypeName sqlTypeName, String format, Object propertyContext) {
      return getFormattedTime(
              transferInputToExprValue(time, sqlTypeName), new ExprStringValue(format))
          .stringValue();
    }
  }
}
