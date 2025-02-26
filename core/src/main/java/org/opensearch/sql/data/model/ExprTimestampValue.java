/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_WITHOUT_NANO;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/** Expression Timestamp Value. */
@RequiredArgsConstructor
public class ExprTimestampValue extends AbstractExprValue {

  private final Instant timestamp;

  /** Constructor. */
  public ExprTimestampValue(String timestamp) {
    try {
      this.timestamp =
          LocalDateTime.parse(timestamp, DATE_TIME_FORMATTER_VARIABLE_NANOS)
              .atZone(ZoneOffset.UTC)
              .toInstant();
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format(
              "timestamp:%s in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'",
              timestamp));
    }
  }

  /** localDateTime Constructor. */
  public ExprTimestampValue(LocalDateTime localDateTime) {
    this.timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant();
  }

  @Override
  public String value() {
    return timestamp.getNano() == 0
        ? DATE_TIME_FORMATTER_WITHOUT_NANO
            .withZone(ZoneOffset.UTC)
            .format(timestamp.truncatedTo(ChronoUnit.SECONDS))
        : DATE_TIME_FORMATTER_VARIABLE_NANOS.withZone(ZoneOffset.UTC).format(timestamp);
  }

  @Override
  public Long valueForCalcite() {
    return timestamp.toEpochMilli();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.TIMESTAMP;
  }

  @Override
  public Instant timestampValue() {
    return timestamp;
  }

  @Override
  public LocalDate dateValue() {
    return timestamp.atZone(ZoneOffset.UTC).toLocalDate();
  }

  @Override
  public LocalTime timeValue() {
    return timestamp.atZone(ZoneOffset.UTC).toLocalTime();
  }

  @Override
  public boolean isDateTime() {
    return true;
  }

  @Override
  public String toString() {
    return String.format("TIMESTAMP '%s'", value());
  }

  @Override
  public int compare(ExprValue other) {
    return timestamp.compareTo(other.timestampValue().atZone(ZoneOffset.UTC).toInstant());
  }

  @Override
  public boolean equal(ExprValue other) {
    return timestamp.equals(other.timestampValue().atZone(ZoneOffset.UTC).toInstant());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp);
  }
}
