/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;

@RequiredArgsConstructor
public class ExprIntervalValue extends AbstractExprValue {
  private final TemporalAmount interval;

  @Override
  public TemporalAmount intervalValue() {
    return interval;
  }

  @Override
  public int compare(ExprValue other) {
    TemporalAmount otherInterval = other.intervalValue();
    if (!interval.getClass().equals(other.intervalValue().getClass())) {
      throw new ExpressionEvaluationException(
          String.format(
              "invalid to compare intervals with units %s and %s",
              unit(), ((ExprIntervalValue) other).unit()));
    }
    return Long.compare(
        interval.get(unit()), otherInterval.get(((ExprIntervalValue) other).unit()));
  }

  @Override
  public boolean equal(ExprValue other) {
    return interval.equals(other.intervalValue());
  }

  @Override
  public TemporalAmount value() {
    return interval;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.INTERVAL;
  }

  /** Util method to get temporal unit stored locally. */
  public TemporalUnit unit() {
    return interval.getUnits().stream()
        .filter(v -> interval.get(v) != 0)
        .findAny()
        .orElse(interval.getUnits().get(0));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(interval);
  }
}
