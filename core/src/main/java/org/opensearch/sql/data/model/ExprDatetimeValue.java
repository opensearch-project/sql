/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_WITH_TZ;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import com.google.common.base.Objects;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

@RequiredArgsConstructor
public class ExprDatetimeValue extends AbstractExprValue {
  private final LocalDateTime datetime;

  /** Constructor with datetime string as input. */
  public ExprDatetimeValue(String datetime) {
    try {
      this.datetime = LocalDateTime.parse(datetime, DATE_TIME_FORMATTER_WITH_TZ);
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format(
              "datetime:%s in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'",
              datetime));
    }
  }

  @Override
  public LocalDateTime datetimeValue() {
    return datetime;
  }

  @Override
  public LocalDate dateValue() {
    return datetime.toLocalDate();
  }

  @Override
  public LocalTime timeValue() {
    return datetime.toLocalTime();
  }

  @Override
  public Instant timestampValue() {
    return ZonedDateTime.of(datetime, UTC_ZONE_ID).toInstant();
  }

  @Override
  public boolean isDateTime() {
    return true;
  }

  @Override
  public int compare(ExprValue other) {
    return datetime.compareTo(other.datetimeValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return datetime.equals(other.datetimeValue());
  }

  @Override
  public String value() {
    return String.format(
        "%s %s",
        DateTimeFormatter.ISO_DATE.format(datetime),
        DateTimeFormatter.ISO_TIME.format(
            (datetime.getNano() == 0) ? datetime.truncatedTo(ChronoUnit.SECONDS) : datetime));
  }

  @Override
  public ExprType type() {
    return ExprCoreType.DATETIME;
  }

  @Override
  public String toString() {
    return String.format("DATETIME '%s'", value());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(datetime);
  }
}
