/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionProperties;

/** Expression Time Value. */
@RequiredArgsConstructor
public class ExprTimeValue extends AbstractExprValue {

  private final LocalTime time;

  /** Constructor of ExprTimeValue. */
  public ExprTimeValue(String time) {
    try {
      this.time = LocalTime.parse(time, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format("time:%s in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'", time));
    }
  }

  @Override
  public String value() {
    return ISO_LOCAL_TIME.format(time);
  }

  @Override
  public Long valueForCalcite() {
    return time.toNanoOfDay() / 1000;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.TIME;
  }

  @Override
  public LocalTime timeValue() {
    return time;
  }

  public LocalDate dateValue(FunctionProperties functionProperties) {
    return LocalDate.now(functionProperties.getQueryStartClock());
  }

  public LocalDateTime datetimeValue(FunctionProperties functionProperties) {
    return LocalDateTime.of(dateValue(functionProperties), timeValue());
  }

  public Instant timestampValue(FunctionProperties functionProperties) {
    return ZonedDateTime.of(dateValue(functionProperties), timeValue(), UTC_ZONE_ID).toInstant();
  }

  @Override
  public boolean isDateTime() {
    return true;
  }

  @Override
  public String toString() {
    return String.format("TIME '%s'", value());
  }

  @Override
  public int compare(ExprValue other) {
    return time.compareTo(other.timeValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return time.equals(other.timeValue());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(time);
  }
}
