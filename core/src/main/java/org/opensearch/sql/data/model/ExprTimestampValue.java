/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_WITHOUT_NANO;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
              .atZone(UTC_ZONE_ID)
              .toInstant();
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format(
              "timestamp:%s in unsupported format, please " + "use yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]",
              timestamp));
    }
  }

  @Override
  public String value() {
    return timestamp.getNano() == 0
        ? DATE_TIME_FORMATTER_WITHOUT_NANO
            .withZone(UTC_ZONE_ID)
            .format(timestamp.truncatedTo(ChronoUnit.SECONDS))
        : DATE_TIME_FORMATTER_VARIABLE_NANOS.withZone(UTC_ZONE_ID).format(timestamp);
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
    return timestamp.atZone(UTC_ZONE_ID).toLocalDate();
  }

  @Override
  public LocalTime timeValue() {
    return timestamp.atZone(UTC_ZONE_ID).toLocalTime();
  }

  @Override
  public LocalDateTime datetimeValue() {
    return timestamp.atZone(UTC_ZONE_ID).toLocalDateTime();
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
    return timestamp.compareTo(other.timestampValue().atZone(UTC_ZONE_ID).toInstant());
  }

  @Override
  public boolean equal(ExprValue other) {
    return timestamp.equals(other.timestampValue().atZone(UTC_ZONE_ID).toInstant());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp);
  }
}
