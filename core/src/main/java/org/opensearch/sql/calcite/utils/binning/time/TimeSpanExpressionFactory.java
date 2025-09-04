/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.time;

import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.CalcitePlanContext;

/**
 * Factory for creating time span expressions for bin command operations. Separated from aggregation
 * span functionality to avoid shared infrastructure.
 */
public class TimeSpanExpressionFactory {

  private final StandardTimeSpanHandler standardHandler = new StandardTimeSpanHandler();
  private final DaySpanHandler dayHandler = new DaySpanHandler();
  private final MonthSpanHandler monthHandler = new MonthSpanHandler();

  /** Creates a bin-specific time span expression for SPL-compatible time binning. */
  public RexNode createTimeSpanExpression(
      RexNode fieldExpr,
      int intervalValue,
      String unit,
      long alignmentOffsetMillis,
      CalcitePlanContext context) {

    TimeUnitConfig config = TimeUnitRegistry.getConfig(unit);
    if (config == null) {
      throw new IllegalArgumentException("Unsupported time unit for bin span: " + unit);
    }

    TimeUnitRegistry.validateSubSecondSpan(config, intervalValue);

    return switch (config) {
      case MICROSECONDS,
          MILLISECONDS,
          CENTISECONDS,
          DECISECONDS,
          SECONDS,
          MINUTES,
          HOURS -> standardHandler.createExpression(
          fieldExpr, intervalValue, config, alignmentOffsetMillis, context);
      case DAYS -> dayHandler.createExpression(fieldExpr, intervalValue, context);
      case MONTHS -> monthHandler.createExpression(fieldExpr, intervalValue, context);
    };
  }

  /** Creates time span expression with time modifier alignment. */
  public RexNode createTimeSpanExpressionWithTimeModifier(
      RexNode fieldExpr,
      int intervalValue,
      String unit,
      String timeModifier,
      CalcitePlanContext context) {

    TimeUnitConfig config = TimeUnitRegistry.getConfig(unit);
    if (config == null) {
      throw new IllegalArgumentException("Unsupported time unit for bin span: " + unit);
    }

    TimeUnitRegistry.validateSubSecondSpan(config, intervalValue);

    // Check if this is an epoch timestamp alignment
    try {
      long epochTimestamp = Long.parseLong(timeModifier);
      return AlignmentHandler.createEpochAlignedSpan(
          fieldExpr, intervalValue, config, epochTimestamp, context);
    } catch (NumberFormatException e) {
      // Time modifier alignment
      return AlignmentHandler.createTimeModifierAlignedSpan(
          fieldExpr, intervalValue, config, timeModifier, context);
    }
  }
}
