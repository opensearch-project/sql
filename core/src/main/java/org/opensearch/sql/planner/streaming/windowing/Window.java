/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * A window represents a range of values by consisting of the lower bound and upper bound for it.
 * To make use of this concept in stream processing, it is called window instead of range. However,
 * as the definition specifies, it is not restricted to time window by design.
 */
@Getter
@ToString
public class Window extends AbstractExprValue {

  /** Constants for caller to use. */
  public static final ExprValue UNBOUND = ExprNullValue.of();
  public static final String START_NAME = "start";
  public static final String END_NAME = "end";

  /** Lower bound (inclusive by default) of the time window. */
  private final ExprValue lowerBound;

  /** Upper bound (exclusive by default) of the time window. */
  private final ExprValue upperBound;

  /**
   * Construct a window by lower and upper bound. For now inclusivity is default value
   * and disallow to customize to simply arithmetic around window.
   *
   * @param lowerBound inclusive lower bound
   * @param upperBound exclusive upper bound
   */
  public Window(ExprValue lowerBound, ExprValue upperBound) {
    Preconditions.checkArgument(isBothBoundValid(lowerBound, upperBound),
        "Lower bound [%s] and upper bound [%s] must be of the same type",
        lowerBound.type(), upperBound.type());

    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  @Override
  public Object value() {
    return ExprValueUtils.tupleValue(ImmutableMap.of(
        START_NAME, lowerBound,
        END_NAME, upperBound));
  }

  @Override
  public ExprType type() {
    return ExprCoreType.WINDOW;
  }

  @Override
  public int compare(ExprValue o) {
    // Define that a window's order is only determined by its upper bound value
    Window other = (Window) o;
    if (upperBound == UNBOUND && other.upperBound == UNBOUND) {
      return 0;
    } else if (upperBound == UNBOUND) {
      return 1;
    } else if (other.upperBound == UNBOUND) {
      return -1;
    }
    return upperBound.compareTo(other.upperBound);
  }

  @Override
  public boolean equal(ExprValue other) {
    return value().equals(other.value());
  }

  private boolean isBothBoundValid(ExprValue lowerBound, ExprValue upperBound) {
    return lowerBound.type() == upperBound.type()
        || lowerBound == UNBOUND || upperBound == UNBOUND;
  }
}
