/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.model;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
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
  private static final ZoneId ZONE = ZoneId.of("UTC");
  /**
   * todo. only support timestamp in format yyyy-MM-dd HH:mm:ss.
   */
  private static final DateTimeFormatter FORMATTER_WITHOUT_NANO = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss");
  private final Instant timestamp;

  private static final DateTimeFormatter FORMATTER_VARIABLE_NANOS;
  private static final int MIN_FRACTION_SECONDS = 0;
  private static final int MAX_FRACTION_SECONDS = 9;

  static {
    FORMATTER_VARIABLE_NANOS = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendFraction(
                ChronoField.NANO_OF_SECOND,
                MIN_FRACTION_SECONDS,
                MAX_FRACTION_SECONDS,
                true)
        .toFormatter();
  }

  /**
   * Constructor.
   */
  public ExprTimestampValue(String timestamp) {
    try {
      this.timestamp = LocalDateTime.parse(timestamp, FORMATTER_VARIABLE_NANOS)
          .atZone(ZONE)
          .toInstant();
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(String.format("timestamp:%s in unsupported format, please "
          + "use yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]", timestamp));
    }

  }

  @Override
  public String value() {
    return timestamp.getNano() == 0 ? FORMATTER_WITHOUT_NANO.withZone(ZONE)
        .format(timestamp.truncatedTo(ChronoUnit.SECONDS))
        : FORMATTER_VARIABLE_NANOS.withZone(ZONE).format(timestamp);
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
