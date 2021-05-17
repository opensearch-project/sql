/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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
