/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_PATTERNS_NANOS_OPTIONAL;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL;

import com.google.common.base.Objects;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/** Expression Date Value. */
@RequiredArgsConstructor
public class ExprDateValue extends AbstractExprValue {

  private final LocalDate date;
  private String datePattern;

  /** Constructor of ExprDateValue. */
  public ExprDateValue(String date) {
    try {
      this.datePattern = determineDatePattern(date);
      this.date = LocalDate.parse(date, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format("date:%s in unsupported format, please use 'yyyy-MM-dd'", date));
    }
  }

  private String determineDatePattern(String date) {
    for (String pattern : DATE_TIME_FORMATTER_PATTERNS_NANOS_OPTIONAL) {
      try {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT)
                .withResolverStyle(ResolverStyle.STRICT);
        if (pattern.contains("HH") || pattern.contains("mm") || pattern.contains("ss")) {
          LocalDateTime.parse(date, formatter);
        } else {
          LocalDate.parse(date, formatter);
        }
        return pattern;
      } catch (DateTimeParseException e) {
        // Ignore and try next pattern
      }
    }
    return null;
  }

  @Override
  public String value() {
    if (this.datePattern == null) {
      return DateTimeFormatter.ISO_LOCAL_DATE.format(date);
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.datePattern, Locale.ROOT);
    if (this.datePattern.contains("HH") || this.datePattern.contains("mm") || this.datePattern.contains("ss")) {
      LocalDateTime dateValueWithDefaultTime = this.date.atTime(0, 0, 0);
      return dateValueWithDefaultTime.format(formatter);
    }
    return this.date.format(formatter);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.DATE;
  }

  @Override
  public LocalDate dateValue() {
    return date;
  }

  @Override
  public LocalTime timeValue() {
    return LocalTime.of(0, 0, 0);
  }

  @Override
  public Instant timestampValue() {
    return ZonedDateTime.of(date, timeValue(), ZoneOffset.UTC).toInstant();
  }

  @Override
  public boolean isDateTime() {
    return true;
  }

  @Override
  public String toString() {
    return String.format("DATE '%s'", DateTimeFormatter.ISO_LOCAL_DATE.format(date));
  }

  @Override
  public int compare(ExprValue other) {
    return date.compareTo(other.dateValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return date.equals(other.dateValue());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(date);
  }
}
