/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.model;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Expression Time Value.
 */
@RequiredArgsConstructor
public class ExprTimeValue extends AbstractExprValue {
  private final LocalTime time;

  private static final DateTimeFormatter FORMATTER_VARIABLE_MICROS;
  private static final int MIN_FRACTION_SECONDS = 0;
  private static final int MAX_FRACTION_SECONDS = 6;

  static {
    FORMATTER_VARIABLE_MICROS = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(
                    ChronoField.MICRO_OF_SECOND,
                    MIN_FRACTION_SECONDS,
                    MAX_FRACTION_SECONDS,
                    true)
            .toFormatter();
  }

  /**
   * Constructor.
   */
  public ExprTimeValue(String time) {
    try {
      this.time = LocalTime.parse(time, FORMATTER_VARIABLE_MICROS);
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(String.format("time:%s in unsupported format, please use "
          + "HH:mm:ss[.SSSSSS]", time));
    }
  }

  @Override
  public String value() {
    return DateTimeFormatter.ISO_LOCAL_TIME.format(time);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.TIME;
  }

  @Override
  public LocalTime timeValue() {
    return time;
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
