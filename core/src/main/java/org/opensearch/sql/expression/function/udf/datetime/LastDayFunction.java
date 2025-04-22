/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.time.LocalDate;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class LastDayFunction extends ImplementorUDF {
  public LastDayFunction() {
    super(new LastDayImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return op -> UserDefinedFunctionUtils.nullableDateUDT;
  }

  public static class LastDayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {

      // Convert the input date to internal expression
      SqlTypeName dateType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().getFirst().getType());
      Expression internalDate =
          Expressions.call(
              LastDayImplementor.class,
              "toInternalDate",
              Expressions.convert_(translatedOperands.getFirst(), String.class),
              Expressions.constant(dateType),
              Expressions.convert_(translator.getRoot(), Object.class));
      Expression lastDay = Expressions.call(BuiltInMethod.LAST_DAY.method, internalDate);

      // Convert the internal expression to output date
      return Expressions.call(
          LastDayImplementor.class, "fromInternalDate", Expressions.convert_(lastDay, int.class));
    }

    public static int toInternalDate(String date, SqlTypeName dateType, Object propertyContext) {
      FunctionProperties properties =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);
      ExprValue value =
          DateTimeApplyUtils.transferInputToExprTimestampValue(date, dateType, properties);
      return SqlFunctions.toInt(java.sql.Date.valueOf(value.dateValue()));
    }

    public static Object fromInternalDate(int date) {
      LocalDate localDate = SqlFunctions.internalToDate(date).toLocalDate();
      return ExprValueUtils.dateValue(localDate).valueForCalcite();
    }
  }
}
