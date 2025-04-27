/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.data.model.ExprStringValue;
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
          "datePart",
          Expressions.convert_(unit, String.class),
          Expressions.convert_(datetime, Object.class),
          Expressions.constant(datetimeType),
          translator.getRoot());
    }

    public static int datePart(
        String part, Object datetime, SqlTypeName datetimeType, DataContext propertyContext) {
      FunctionProperties properties =
          UserDefinedFunctionUtils.restoreFunctionProperties(propertyContext);

      // This throws errors when date_part expects a date but gets a time, or vice versa.
      if (SqlTypeFamily.STRING.equals(datetimeType.getFamily())
          || SqlTypeFamily.CHARACTER.equals(datetimeType.getFamily())) {
        ensureDatetimeParsable(part, datetime.toString());
      }

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
