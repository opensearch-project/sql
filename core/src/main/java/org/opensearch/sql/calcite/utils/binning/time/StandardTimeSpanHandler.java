/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.time;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.binning.BinConstants;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/** Handler for standard time units (microseconds through hours). */
public class StandardTimeSpanHandler {

  public RexNode createExpression(
      RexNode fieldExpr,
      int intervalValue,
      TimeUnitConfig config,
      long alignmentOffsetMillis,
      CalcitePlanContext context) {

    // Convert timestamp to target unit
    RexNode epochValue = convertToTargetUnit(fieldExpr, config, context);

    // Apply alignment offset
    long alignmentOffset = convertAlignmentOffset(alignmentOffsetMillis, config);
    RexNode adjustedValue = applyAlignmentOffset(epochValue, alignmentOffset, context);

    // Perform binning
    RexNode binValue = performBinning(adjustedValue, intervalValue, context);

    // Add back alignment offset
    if (alignmentOffset != 0) {
      binValue =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder,
              BuiltinFunctionName.ADD,
              binValue,
              context.relBuilder.literal(alignmentOffset));
    }

    // Convert back to timestamp
    return convertFromTargetUnit(binValue, config, context);
  }

  private RexNode convertToTargetUnit(
      RexNode fieldExpr, TimeUnitConfig config, CalcitePlanContext context) {

    RexNode epochSeconds =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    // For sub-second units, work in milliseconds
    if (isSubSecondUnit(config)) {
      RexNode epochMillis =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder,
              BuiltinFunctionName.MULTIPLY,
              epochSeconds,
              context.relBuilder.literal(1000L));

      if (config.getDivisionFactor() == 1) {
        return epochMillis;
      } else if (config.getDivisionFactor() > 1) {
        return PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.DIVIDE,
            epochMillis,
            context.relBuilder.literal(config.getDivisionFactor()));
      } else {
        // Microseconds
        return PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.MULTIPLY,
            epochMillis,
            context.relBuilder.literal(BinConstants.MICROS_PER_MILLI));
      }
    } else {
      // For second and larger units, work in seconds
      if (config.getDivisionFactor() == 1) {
        return epochSeconds;
      } else {
        return PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.DIVIDE,
            epochSeconds,
            context.relBuilder.literal(config.getDivisionFactor()));
      }
    }
  }

  private RexNode convertFromTargetUnit(
      RexNode binValue, TimeUnitConfig config, CalcitePlanContext context) {

    if (isSubSecondUnit(config)) {
      RexNode binMillis;
      if (config.getDivisionFactor() == 1) {
        binMillis = binValue;
      } else if (config.getDivisionFactor() > 1) {
        binMillis =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.MULTIPLY,
                binValue,
                context.relBuilder.literal(config.getDivisionFactor()));
      } else {
        // Microseconds
        binMillis =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.DIVIDE,
                binValue,
                context.relBuilder.literal(BinConstants.MICROS_PER_MILLI));
      }

      RexNode binSeconds =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder,
              BuiltinFunctionName.DIVIDE,
              binMillis,
              context.relBuilder.literal(1000L));

      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binSeconds);
    } else {
      RexNode binSeconds;
      if (config.getDivisionFactor() == 1) {
        binSeconds = binValue;
      } else {
        binSeconds =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.MULTIPLY,
                binValue,
                context.relBuilder.literal(config.getDivisionFactor()));
      }

      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binSeconds);
    }
  }

  private RexNode applyAlignmentOffset(
      RexNode epochValue, long alignmentOffset, CalcitePlanContext context) {
    if (alignmentOffset == 0) {
      return epochValue;
    }
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder,
        BuiltinFunctionName.SUBTRACT,
        epochValue,
        context.relBuilder.literal(alignmentOffset));
  }

  private RexNode performBinning(
      RexNode adjustedValue, int intervalValue, CalcitePlanContext context) {
    RexNode intervalLiteral = context.relBuilder.literal(intervalValue);
    RexNode divided =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.DIVIDE, adjustedValue, intervalLiteral);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder, BuiltinFunctionName.MULTIPLY, floored, intervalLiteral);
  }

  private long convertAlignmentOffset(long offsetMillis, TimeUnitConfig config) {
    if (offsetMillis == 0 || !config.supportsAlignment()) {
      return 0;
    }

    return switch (config) {
      case MICROSECONDS -> offsetMillis * BinConstants.MICROS_PER_MILLI;
      case MILLISECONDS -> offsetMillis;
      case CENTISECONDS -> offsetMillis / BinConstants.MILLIS_PER_CENTISECOND;
      case DECISECONDS -> offsetMillis / BinConstants.MILLIS_PER_DECISECOND;
      case SECONDS -> offsetMillis / BinConstants.MILLIS_PER_SECOND;
      case MINUTES -> offsetMillis / BinConstants.MILLIS_PER_MINUTE;
      case HOURS -> offsetMillis / BinConstants.MILLIS_PER_HOUR;
      default -> 0;
    };
  }

  private boolean isSubSecondUnit(TimeUnitConfig config) {
    return config == TimeUnitConfig.MICROSECONDS
        || config == TimeUnitConfig.MILLISECONDS
        || config == TimeUnitConfig.CENTISECONDS
        || config == TimeUnitConfig.DECISECONDS;
  }
}
