/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.QueryFunctionProperties;

@ExtendWith(MockitoExtension.class)
public class DateTimeTestBase extends ExpressionTestBase {

  protected final BuiltinFunctionRepository functionRepository
      = BuiltinFunctionRepository.getInstance();

  @Mock
  protected Environment<Expression, ExprValue> env;


  protected static FunctionProperties functionProperties;

  @BeforeAll
  public static void setup() {
    functionProperties = new QueryFunctionProperties();
  }

  protected Expression nullRef = DSL.literal(ExprNullValue.of());

  protected Expression missingRef = DSL.literal(ExprMissingValue.of());

  protected ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }

  protected LocalDateTime fromUnixTime(Double value) {
    return fromUnixTime(DSL.literal(value)).valueOf().datetimeValue();
  }

  protected FunctionExpression fromUnixTime(Expression value) {
    return (FunctionExpression)
        functionRepository.compile(functionProperties,
            BuiltinFunctionName.FROM_UNIXTIME.getName(), List.of(value));
  }

  protected FunctionExpression fromUnixTime(Expression value, Expression format) {
    return (FunctionExpression)
        functionRepository.compile(
            functionProperties,
            BuiltinFunctionName.FROM_UNIXTIME.getName(), List.of(value, format));
  }

  protected LocalDateTime fromUnixTime(Long value) {
    return fromUnixTime(DSL.literal(value)).valueOf().datetimeValue();
  }

  protected String fromUnixTime(Long value, String format) {
    return fromUnixTime(DSL.literal(value), DSL.literal(format))
        .valueOf().stringValue();
  }

  protected String fromUnixTime(Double value, String format) {
    return fromUnixTime(DSL.literal(value), DSL.literal(format))
        .valueOf().stringValue();
  }

  protected FunctionExpression maketime(Expression hour, Expression minute, Expression second) {
    return (FunctionExpression)
        functionRepository.compile(
            functionProperties,
            BuiltinFunctionName.MAKETIME.getName(), List.of(hour, minute, second));
  }


  protected LocalTime maketime(Double hour, Double minute, Double second) {
    return maketime(DSL.literal(hour), DSL.literal(minute), DSL.literal(second))
        .valueOf().timeValue();
  }

  protected FunctionExpression makedate(Expression year, Expression dayOfYear) {
    return (FunctionExpression) functionRepository.compile(
        functionProperties,
        BuiltinFunctionName.MAKEDATE.getName(), List.of(year, dayOfYear));
  }

  protected LocalDate makedate(double year, double dayOfYear) {
    return makedate(DSL.literal(year), DSL.literal(dayOfYear)).valueOf(null).dateValue();
  }

  protected FunctionExpression period_add(Expression period, Expression months) {
    return (FunctionExpression) functionRepository.compile(
        functionProperties,
        BuiltinFunctionName.PERIOD_ADD.getName(), List.of(period, months));
  }

  protected Integer period_add(Integer period, Integer months) {
    return period_add(DSL.literal(period), DSL.literal(months))
        .valueOf().integerValue();
  }

  protected FunctionExpression period_diff(Expression first, Expression second) {
    return (FunctionExpression) functionRepository.compile(
        functionProperties,
        BuiltinFunctionName.PERIOD_DIFF.getName(), List.of(first, second));
  }

  protected Integer period_diff(Integer first, Integer second) {
    return period_diff(DSL.literal(first), DSL.literal(second))
        .valueOf().integerValue();
  }

  protected FunctionExpression unixTimeStampExpr() {
    return (FunctionExpression) functionRepository.compile(
        functionProperties, BuiltinFunctionName.UNIX_TIMESTAMP.getName(), List.of());
  }

  protected Long unixTimeStamp() {
    return unixTimeStampExpr().valueOf().longValue();
  }

  protected FunctionExpression unixTimeStampOf(Expression value) {
    return (FunctionExpression)
        functionRepository.compile(functionProperties,
            BuiltinFunctionName.UNIX_TIMESTAMP.getName(), List.of(value));
  }


  protected Double unixTimeStampOf(Double value) {
    return unixTimeStampOf(DSL.literal(value)).valueOf().doubleValue();
  }

  protected Double unixTimeStampOf(LocalDate value) {
    return unixTimeStampOf(DSL.literal(new ExprDateValue(value)))
        .valueOf().doubleValue();
  }

  protected Double unixTimeStampOf(LocalDateTime value) {
    return unixTimeStampOf(DSL.literal(new ExprDatetimeValue(value)))
        .valueOf().doubleValue();
  }

  protected Double unixTimeStampOf(Instant value) {
    return unixTimeStampOf(DSL.literal(new ExprTimestampValue(value)))
        .valueOf().doubleValue();
  }
}
