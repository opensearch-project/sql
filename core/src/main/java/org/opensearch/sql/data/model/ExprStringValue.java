/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/** Expression String Value. */
@RequiredArgsConstructor
public class ExprStringValue extends AbstractExprValue {
  private final String value;

  @Override
  public Object value() {
    return value;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  @Override
  public String stringValue() {
    return value;
  }

  @Override
  public Instant timestampValue() {
    try {
      return new ExprTimestampValue(value).timestampValue();
    } catch (SemanticCheckException e) {
      return new ExprTimestampValue(
              LocalDateTime.of(new ExprDateValue(value).dateValue(), LocalTime.of(0, 0, 0)))
          .timestampValue();
    }
  }

  @Override
  public LocalDate dateValue() {
    try {
      return new ExprTimestampValue(value).dateValue();
    } catch (SemanticCheckException e) {
      return new ExprDateValue(value).dateValue();
    }
  }

  @Override
  public LocalTime timeValue() {
    try {
      return new ExprTimestampValue(value).timeValue();
    } catch (SemanticCheckException e) {
      return new ExprTimeValue(value).timeValue();
    }
  }

  @Override
  public String toString() {
    return String.format("\"%s\"", value);
  }

  @Override
  public int compare(ExprValue other) {
    return value.compareTo(other.stringValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return value.equals(other.stringValue());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}
