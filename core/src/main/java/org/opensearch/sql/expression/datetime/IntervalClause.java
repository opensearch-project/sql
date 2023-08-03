/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.opensearch.sql.data.model.ExprValueUtils.getIntegerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getLongValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getStringValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import java.time.Duration;
import java.time.Period;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprIntervalValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;

@UtilityClass
public class IntervalClause {

  private static final String MICRO_SECOND = "microsecond";
  private static final String SECOND = "second";
  private static final String MINUTE = "minute";
  private static final String HOUR = "hour";
  private static final String DAY = "day";
  private static final String WEEK = "week";
  private static final String MONTH = "month";
  private static final String QUARTER = "quarter";
  private static final String YEAR = "year";

  public void register(BuiltinFunctionRepository repository) {
    repository.register(interval());
  }

  private DefaultFunctionResolver interval() {
    return define(
        BuiltinFunctionName.INTERVAL.getName(),
        impl(nullMissingHandling(IntervalClause::interval), INTERVAL, INTEGER, STRING),
        impl(nullMissingHandling(IntervalClause::interval), INTERVAL, LONG, STRING));
  }

  private ExprValue interval(ExprValue value, ExprValue unit) {
    switch (getStringValue(unit).toLowerCase()) {
      case MICRO_SECOND:
        return microsecond(value);
      case SECOND:
        return second(value);
      case MINUTE:
        return minute(value);
      case HOUR:
        return hour(value);
      case DAY:
        return day(value);
      case WEEK:
        return week(value);
      case MONTH:
        return month(value);
      case QUARTER:
        return quarter(value);
      case YEAR:
        return year(value);
      default:
        throw new ExpressionEvaluationException(
            String.format("interval unit %s is not supported", getStringValue(unit)));
    }
  }

  private ExprValue microsecond(ExprValue value) {
    return new ExprIntervalValue(Duration.ofNanos(getLongValue(value) * 1000));
  }

  private ExprValue second(ExprValue value) {
    return new ExprIntervalValue(Duration.ofSeconds(getLongValue(value)));
  }

  private ExprValue minute(ExprValue value) {
    return new ExprIntervalValue(Duration.ofMinutes(getLongValue(value)));
  }

  private ExprValue hour(ExprValue value) {
    return new ExprIntervalValue(Duration.ofHours(getLongValue(value)));
  }

  private ExprValue day(ExprValue value) {
    return new ExprIntervalValue(Period.ofDays(getIntegerValue(value)));
  }

  private ExprValue week(ExprValue value) {
    return new ExprIntervalValue(Period.ofWeeks(getIntegerValue(value)));
  }

  private ExprValue month(ExprValue value) {
    return new ExprIntervalValue(Period.ofMonths(getIntegerValue(value)));
  }

  private ExprValue quarter(ExprValue value) {
    return new ExprIntervalValue(Period.ofMonths(getIntegerValue(value) * 3));
  }

  private ExprValue year(ExprValue value) {
    return new ExprIntervalValue(Period.ofYears(getIntegerValue(value)));
  }
}
