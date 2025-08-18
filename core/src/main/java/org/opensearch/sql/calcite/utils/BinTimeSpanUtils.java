/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.binning.time.TimeSpanExpressionFactory;

/**
 * Simplified facade for time span expressions in bin command operations. This is completely
 * separate from the aggregation span functionality to avoid shared infrastructure that could break
 * customer queries.
 */
public class BinTimeSpanUtils {

  private static final TimeSpanExpressionFactory factory = new TimeSpanExpressionFactory();

  /**
   * Creates a bin-specific time span expression for SPL-compatible time binning.
   *
   * @param fieldExpr The field expression to bin
   * @param intervalValue The interval value
   * @param unit The time unit (e.g., "h", "m", "s")
   * @param alignmentOffsetMillis Alignment offset in milliseconds
   * @param context The Calcite plan context
   * @return RexNode representing the time span expression
   */
  public static RexNode createBinTimeSpanExpression(
      RexNode fieldExpr,
      int intervalValue,
      String unit,
      long alignmentOffsetMillis,
      CalcitePlanContext context) {

    return factory.createTimeSpanExpression(
        fieldExpr, intervalValue, unit, alignmentOffsetMillis, context);
  }

  /**
   * Creates a bin-specific time span expression with time modifier alignment. Handles SPL time
   * modifiers like @d, @d+4h, @d-1h and epoch timestamps.
   *
   * @param fieldExpr The field expression to bin
   * @param intervalValue The interval value
   * @param unit The time unit (e.g., "h", "m", "s")
   * @param timeModifier The time modifier or epoch timestamp
   * @param context The Calcite plan context
   * @return RexNode representing the time span expression
   */
  public static RexNode createBinTimeSpanExpressionWithTimeModifier(
      RexNode fieldExpr,
      int intervalValue,
      String unit,
      String timeModifier,
      CalcitePlanContext context) {

    return factory.createTimeSpanExpressionWithTimeModifier(
        fieldExpr, intervalValue, unit, timeModifier, context);
  }
}
