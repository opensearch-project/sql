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
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Expression Timestamp Value.
 */
@RequiredArgsConstructor
public class ExprTimestampValue extends AbstractExprValue {
  /**
   * todo. only support UTC now.
   */
  public static final ZoneId ZONE = ZoneId.of("UTC");

  private final Instant timestamp;

  /**
   * Constructor.
   */
  public ExprTimestampValue(String timestamp) {
    try {
      this.timestamp = LocalDateTime.parse(timestamp, DATE_TIME_FORMATTER_VARIABLE_NANOS)
          .atZone(ZONE)
          .toInstant();
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(String.format("timestamp:%s in unsupported format, please "
          + "use yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]", timestamp));
    }

  }

  @Override
  public String value() {
    return timestamp.getNano() == 0 ? DATE_TIME_FORMATTER_WITHOUT_NANO.withZone(ZONE)
        .format(timestamp.truncatedTo(ChronoUnit.SECONDS))
        : DATE_TIME_FORMATTER_VARIABLE_NANOS.withZone(ZONE).format(timestamp);
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
    return timestamp.atZone(ZONE).toLocalDate();
  }

  @Override
  public LocalTime timeValue() {
    return timestamp.atZone(ZONE).toLocalTime();
  }

  @Override
  public LocalDateTime datetimeValue() {
    return timestamp.atZone(ZONE).toLocalDateTime();
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
    return timestamp.compareTo(other.timestampValue().atZone(ZONE).toInstant());
  }

  @Override
  public boolean equal(ExprValue other) {
    return timestamp.equals(other.timestampValue().atZone(ZONE).toInstant());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp);
  }
}
