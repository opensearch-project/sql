/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import inet.ipaddr.IPAddress;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/** The definition of the Expression Value. */
public interface ExprValue extends Serializable, Comparable<ExprValue> {
  /** Get the Object value of the Expression Value. */
  Object value();

  /** Get the {@link ExprCoreType} of the Expression Value. */
  ExprType type();

  /**
   * Is null value.
   *
   * @return true: is null value, otherwise false
   */
  default boolean isNull() {
    return false;
  }

  /**
   * Is missing value.
   *
   * @return true: is missing value, otherwise false
   */
  default boolean isMissing() {
    return false;
  }

  /**
   * Is Number value.
   *
   * @return true: is number value, otherwise false
   */
  default boolean isNumber() {
    return false;
  }

  /**
   * Is Datetime value.
   *
   * @return true: is a datetime value, otherwise false
   */
  default boolean isDateTime() {
    return false;
  }

  /** Get the {@link BindingTuple}. */
  default BindingTuple bindingTuples() {
    return BindingTuple.EMPTY;
  }

  /** Get byte value. */
  default Byte byteValue() {
    throw new ExpressionEvaluationException(
        "invalid to get byteValue from value of type " + type());
  }

  /** Get short value. */
  default Short shortValue() {
    throw new ExpressionEvaluationException(
        "invalid to get shortValue from value of type " + type());
  }

  /** Get integer value. */
  default Integer integerValue() {
    throw new ExpressionEvaluationException(
        "invalid to get integerValue from value of type " + type());
  }

  /** Get long value. */
  default Long longValue() {
    throw new ExpressionEvaluationException(
        "invalid to get longValue from value of type " + type());
  }

  /** Get float value. */
  default Float floatValue() {
    throw new ExpressionEvaluationException(
        "invalid to get floatValue from value of type " + type());
  }

  /** Get float value. */
  default Double doubleValue() {
    throw new ExpressionEvaluationException(
        "invalid to get doubleValue from value of type " + type());
  }

  /** Get IP address value. */
  default IPAddress ipValue() {
    throw new ExpressionEvaluationException("invalid to get ipValue from value of type " + type());
  }

  /** Get string value. */
  default String stringValue() {
    throw new ExpressionEvaluationException(
        "invalid to get stringValue from value of type " + type());
  }

  /** Get boolean value. */
  default Boolean booleanValue() {
    throw new ExpressionEvaluationException(
        "invalid to get booleanValue from value of type " + type());
  }

  /** Get timestamp value. */
  default Instant timestampValue() {
    throw new ExpressionEvaluationException(
        "invalid to get timestampValue from value of type " + type());
  }

  /** Get time value. */
  default LocalTime timeValue() {
    throw new ExpressionEvaluationException(
        "invalid to get timeValue from value of type " + type());
  }

  /** Get date value. */
  default LocalDate dateValue() {
    throw new ExpressionEvaluationException(
        "invalid to get dateValue from value of type " + type());
  }

  /** Get datetime value. */
  default LocalDateTime datetimeValue() {
    throw new ExpressionEvaluationException(
        "invalid to get datetimeValue from value of type " + type());
  }

  /** Get interval value. */
  default TemporalAmount intervalValue() {
    throw new ExpressionEvaluationException(
        "invalid to get intervalValue from value of type " + type());
  }

  /** Get map value. */
  default Map<String, ExprValue> tupleValue() {
    throw new ExpressionEvaluationException(
        "invalid to get tupleValue from value of type " + type());
  }

  /** Get collection value. */
  default List<ExprValue> collectionValue() {
    throw new ExpressionEvaluationException(
        "invalid to get collectionValue from value of type " + type());
  }

  /**
   * Get the value specified by key from {@link ExprTupleValue}. This method only be implemented in
   * {@link ExprTupleValue}.
   */
  default ExprValue keyValue(String key) {
    return ExprMissingValue.of();
  }
}
