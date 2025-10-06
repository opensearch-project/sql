/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import lombok.Getter;

/** Data class to hold parsed span information. */
@Getter
public class SpanInfo {
  private final SpanType type;
  private final double value;
  private final String unit;
  private final double coefficient; // For log spans
  private final double base; // For log spans

  /** Constructor for numeric and time spans. */
  public SpanInfo(SpanType type, double value, String unit) {
    this.type = type;
    this.value = value;
    this.unit = unit;
    this.coefficient = 1.0;
    this.base = 10.0;
  }

  /** Constructor for logarithmic spans. */
  public SpanInfo(SpanType type, double coefficient, double base) {
    this.type = type;
    this.value = 0;
    this.unit = null;
    this.coefficient = coefficient;
    this.base = base;
  }
}
