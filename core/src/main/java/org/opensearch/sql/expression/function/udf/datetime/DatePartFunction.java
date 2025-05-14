/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeConversionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Implementations of date-part-related functions:
 *
 * <ul>
 *   <li>YEAR
 *   <li>QUARTER
 *   <li>MONTH
 *   <li>WEEK
 *   <li>DAY
 *   <li>DAYOFYEAR
 *   <li>DAYOFMONTH
 *   <li>DAYOFWEEK
 *   <li>HOUR
 *   <li>HOUROFDAY
 *   <li>MINUTE
 *   <li>MINUTEOFDAY
 *   <li>MIBUTEOFHOUR
 *   <li>SECOND
 *   <li>MICROSECOND
 * </ul>
 *
 * Signatures:
 *
 * <ul>
 *   <li>Day-level extractions: DATE/TIMESTAMP/STRING -> INTEGER
 *   <li>Time-level extractions: TIME/TIMESTAMP/STRING -> INTEGER
 * </ul>
 */
public class DatePartFunction extends ImplementorUDF {
  public DatePartFunction(TimeUnit timeUnit) {
    super(new DatePartImplementor(timeUnit), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.INTEGER_FORCE_NULLABLE;
  }

  @RequiredArgsConstructor
  public static class DatePartImplementor implements NotNullImplementor {
    private final TimeUnit timeUnit;

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> operands) {

      Expression unit = Expressions.constant(timeUnit.name());
      List<Expression> exprOperands = UserDefinedFunctionUtils.convertToExprValues(operands, call);
      List<Expression> exprOperandsWithProperties =
          UserDefinedFunctionUtils.prependFunctionProperties(exprOperands, translator);
      return Expressions.call(
          DatePartImplementor.class,
          "datePart",
          exprOperandsWithProperties.get(0),
          exprOperandsWithProperties.get(1),
          Expressions.convert_(unit, String.class));
    }

    public static int datePart(FunctionProperties properties, ExprValue datetime, String part) {

      // This throws errors when date_part expects a date but gets a time, or vice versa.
      if (ExprCoreType.STRING.isCompatible(datetime.type())) {
        ensureDatetimeParsable(part, datetime.stringValue());
      }

      ExprTimestampValue candidate =
          DateTimeConversionUtils.forceConvertToTimestampValue(datetime, properties);

      if (datetime.type() == ExprCoreType.TIME) {
        return DateTimeFunctions.exprExtractForTime(
                properties, new ExprStringValue(part), candidate)
            .integerValue();
      }
      return DateTimeFunctions.formatExtractFunction(new ExprStringValue(part), candidate)
          .integerValue();
    }

    private static void ensureDatetimeParsable(String part, String datetime) {
      final Set<String> TIME_UNITS = Set.of("MICROSECOND", "SECOND", "MINUTE", "HOUR");
      part = part.toUpperCase(Locale.ROOT);
      if (TIME_UNITS.contains(part)) {
        // Ensure the input is parsable as a time value
        fromObjectValue(datetime, ExprCoreType.TIME);
      } else {
        // Ensure the input is parsable as a date value
        fromObjectValue(datetime, ExprCoreType.DATE);
      }
    }
  }
}
