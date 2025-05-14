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
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * <code>DATETIME(timestamp)/ DATETIME(date, to_timezone) </code>Converts the datetime to a new
 * timezone
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(TIMESTAMP/STRING) -> TIMESTAMP
 *   <li>(TIMESTAMP/STRING, STRING) -> TIMESTAMP
 * </ul>
 */
public class DatetimeFunction extends ImplementorUDF {
  public DatetimeFunction() {
    super(new DatetimeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker) OperandTypes.TIMESTAMP_STRING.or(OperandTypes.TIMESTAMP));
  }

  public static class DatetimeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(DatetimeImplementor.class, "datetime", translatedOperands);
    }

    public static String datetime(String timestamp) {
      ExprValue argTimestampExpr = new ExprStringValue(timestamp);
      ExprValue datetimeExpr;
      datetimeExpr = DateTimeFunctions.exprDateTimeNoTimezone(argTimestampExpr);
      return (String) datetimeExpr.valueForCalcite();
    }

    public static String datetime(String timestamp, String timezone) {
      ExprValue timestampExpr = new ExprStringValue(timestamp);
      ExprValue datetimeExpr =
          DateTimeFunctions.exprDateTime(timestampExpr, new ExprStringValue(timezone));
      return (String) datetimeExpr.valueForCalcite();
    }
  }
}
