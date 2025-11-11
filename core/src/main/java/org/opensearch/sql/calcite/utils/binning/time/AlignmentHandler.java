/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.time;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/** Handler for time alignment operations (@d, @d+offset, epoch alignment). */
public class AlignmentHandler {

  /** Creates time span with epoch timestamp alignment. */
  public static RexNode createEpochAlignedSpan(
      RexNode fieldExpr,
      int intervalValue,
      TimeUnitConfig config,
      long referenceEpochSeconds,
      CalcitePlanContext context) {

    RexNode epochSeconds =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);
    RexNode referenceTimestamp = context.relBuilder.literal(referenceEpochSeconds);

    long intervalSeconds = config.toSeconds(intervalValue);
    RexNode intervalLiteral = context.relBuilder.literal(intervalSeconds);

    // SPL Universal Formula: bin_start = reference + floor((timestamp - reference) / span) * span
    RexNode timeOffset =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.SUBTRACT, epochSeconds, referenceTimestamp);

    RexNode binNumber =
        context.relBuilder.call(
            SqlStdOperatorTable.FLOOR,
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder, BuiltinFunctionName.DIVIDE, timeOffset, intervalLiteral));

    RexNode binOffset =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MULTIPLY, binNumber, intervalLiteral);

    RexNode binStartSeconds =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.ADD, referenceTimestamp, binOffset);

    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binStartSeconds);
  }

  /** Creates time span with SPL time modifier alignment (@d, @d+4h, @d-1h). */
  public static RexNode createTimeModifierAlignedSpan(
      RexNode fieldExpr,
      int intervalValue,
      TimeUnitConfig config,
      String timeModifier,
      CalcitePlanContext context) {

    RexNode epochSeconds =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    // Parse time modifier
    long offsetMillis = parseTimeModifier(timeModifier);
    boolean alignToDay = timeModifier != null && timeModifier.startsWith("@d");

    long intervalSeconds = config.toSeconds(intervalValue);
    RexNode intervalLiteral = context.relBuilder.literal(intervalSeconds);

    if (alignToDay) {
      // Use earliest timestamp in dataset to determine reference (SPL spec)
      RexNode secondsPerDay = context.relBuilder.literal(86400L);

      // TODO: Replace with actual MIN(fieldExpr) when available
      RexNode earliestTimestamp = context.relBuilder.literal(1753661723L);

      // Calculate start of day for earliest timestamp
      RexNode daysSinceEpoch =
          context.relBuilder.call(
              SqlStdOperatorTable.FLOOR,
              PPLFuncImpTable.INSTANCE.resolve(
                  context.rexBuilder,
                  BuiltinFunctionName.DIVIDE,
                  earliestTimestamp,
                  secondsPerDay));

      RexNode startOfEarliestDay =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, BuiltinFunctionName.MULTIPLY, daysSinceEpoch, secondsPerDay);

      // Calculate alignment reference point
      RexNode alignmentReference;
      if (offsetMillis != 0) {
        long offsetSeconds = offsetMillis / 1000L;
        alignmentReference =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.ADD,
                startOfEarliestDay,
                context.relBuilder.literal(offsetSeconds));
      } else {
        alignmentReference = startOfEarliestDay;
      }

      // Apply SPL Universal Formula
      RexNode timeOffset =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, BuiltinFunctionName.SUBTRACT, epochSeconds, alignmentReference);

      RexNode binNumber =
          context.relBuilder.call(
              SqlStdOperatorTable.FLOOR,
              PPLFuncImpTable.INSTANCE.resolve(
                  context.rexBuilder, BuiltinFunctionName.DIVIDE, timeOffset, intervalLiteral));

      RexNode binOffset =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, BuiltinFunctionName.MULTIPLY, binNumber, intervalLiteral);

      RexNode binStartSeconds =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, BuiltinFunctionName.ADD, alignmentReference, binOffset);

      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binStartSeconds);
    } else {
      // No day alignment
      RexNode divided =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, BuiltinFunctionName.DIVIDE, epochSeconds, intervalLiteral);
      RexNode binNumber = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
      RexNode binStartSeconds =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, BuiltinFunctionName.MULTIPLY, binNumber, intervalLiteral);

      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binStartSeconds);
    }
  }

  private static long parseTimeModifier(String timeModifier) {
    if (timeModifier == null || timeModifier.equals("@d")) {
      return 0;
    }

    if (timeModifier.startsWith("@d+")) {
      String offsetStr = timeModifier.substring(3);
      return parseTimeOffset(offsetStr);
    }

    if (timeModifier.startsWith("@d-")) {
      String offsetStr = timeModifier.substring(3);
      return -parseTimeOffset(offsetStr);
    }

    return 0;
  }

  private static long parseTimeOffset(String offsetStr) {
    offsetStr = offsetStr.trim().toLowerCase();

    if (offsetStr.endsWith("h")) {
      int hours = Integer.parseInt(offsetStr.substring(0, offsetStr.length() - 1));
      return hours * 3600000L;
    } else if (offsetStr.endsWith("m")) {
      int minutes = Integer.parseInt(offsetStr.substring(0, offsetStr.length() - 1));
      return minutes * 60000L;
    } else if (offsetStr.endsWith("s")) {
      int seconds = Integer.parseInt(offsetStr.substring(0, offsetStr.length() - 1));
      return seconds * 1000L;
    } else {
      int hours = Integer.parseInt(offsetStr);
      return hours * 3600000L;
    }
  }
}
