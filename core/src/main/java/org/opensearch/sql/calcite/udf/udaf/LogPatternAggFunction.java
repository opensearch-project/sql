/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.calcite.udf.udaf.LogPatternAggFunction.LogParserAccumulator;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.common.patterns.PatternAggregationHelpers;

/**
 * User-defined aggregate function for log pattern extraction using the Brain algorithm. This UDAF
 * is used for in-memory pattern aggregation in Calcite. For OpenSearch scripted metric pushdown,
 * see {@link PatternAggregationHelpers} which provides the same logic with Map-based state.
 *
 * <p>Both implementations share the same underlying logic through {@link PatternAggregationHelpers}
 * to ensure consistency.
 */
public class LogPatternAggFunction implements UserDefinedAggFunction<LogParserAccumulator> {
  private int bufferLimit = 100000;
  private int maxSampleCount = 10;
  private boolean showNumberedToken = false;
  private int variableCountThreshold = BrainLogParser.DEFAULT_VARIABLE_COUNT_THRESHOLD;
  private double thresholdPercentage = BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE;

  @Override
  public LogParserAccumulator init() {
    return new LogParserAccumulator();
  }

  @Override
  public Object result(LogParserAccumulator acc) {
    if (acc.isEmpty()) {
      return null;
    }
    return acc.value(
        maxSampleCount, variableCountThreshold, thresholdPercentage, showNumberedToken);
  }

  @Override
  public LogParserAccumulator add(LogParserAccumulator acc, Object... values) {
    throw new SyntaxCheckException(
        "Unsupported function signature for pattern aggregate. Valid parameters include (field:"
            + " required string), (max_sample_count: required integer),"
            + " (buffer_limit: required integer), (show_numbered_token: required boolean),"
            + " [variable_count_threshold: optional integer],"
            + " [frequency_threshold_percentage: optional double]");
  }

  public LogParserAccumulator add(
      LogParserAccumulator acc,
      String field,
      int maxSampleCount,
      int bufferLimit,
      boolean showNumberedToken,
      BigDecimal thresholdPercentage,
      int variableCountThreshold) {
    return add(
        acc,
        field,
        maxSampleCount,
        bufferLimit,
        showNumberedToken,
        thresholdPercentage.doubleValue(),
        variableCountThreshold);
  }

  public LogParserAccumulator add(
      LogParserAccumulator acc,
      String field,
      int maxSampleCount,
      int bufferLimit,
      boolean showNumberedToken,
      double thresholdPercentage,
      int variableCountThreshold) {
    if (Objects.isNull(field)) {
      return acc;
    }
    // Store parameters for result() phase
    this.bufferLimit = bufferLimit;
    this.maxSampleCount = maxSampleCount;
    this.showNumberedToken = showNumberedToken;
    this.variableCountThreshold = variableCountThreshold;
    this.thresholdPercentage = thresholdPercentage;

    // Delegate to shared helper logic
    PatternAggregationHelpers.addLogToPattern(
        acc.state, field, maxSampleCount, bufferLimit, variableCountThreshold, thresholdPercentage);
    return acc;
  }

  public LogParserAccumulator add(
      LogParserAccumulator acc,
      String field,
      int maxSampleCount,
      int bufferLimit,
      boolean showNumberedToken,
      int variableCountThreshold) {
    return add(
        acc,
        field,
        maxSampleCount,
        bufferLimit,
        showNumberedToken,
        this.thresholdPercentage,
        variableCountThreshold);
  }

  public LogParserAccumulator add(
      LogParserAccumulator acc,
      String field,
      int maxSampleCount,
      int bufferLimit,
      boolean showNumberedToken,
      BigDecimal thresholdPercentage) {
    return add(
        acc,
        field,
        maxSampleCount,
        bufferLimit,
        showNumberedToken,
        thresholdPercentage.doubleValue(),
        this.variableCountThreshold);
  }

  public LogParserAccumulator add(
      LogParserAccumulator acc,
      String field,
      int maxSampleCount,
      int bufferLimit,
      boolean showNumberedToken) {
    return add(
        acc,
        field,
        maxSampleCount,
        bufferLimit,
        showNumberedToken,
        this.thresholdPercentage,
        this.variableCountThreshold);
  }

  /**
   * Accumulator for log pattern aggregation. This is a thin wrapper around the Map-based state used
   * by {@link PatternAggregationHelpers}, providing type safety for Calcite UDAF while reusing the
   * same underlying logic.
   */
  public static class LogParserAccumulator implements Accumulator {
    /** The underlying state map, compatible with PatternAggregationHelpers */
    final Map<String, Object> state;

    public LogParserAccumulator() {
      this.state = PatternAggregationHelpers.initPatternAccumulator();
    }

    @SuppressWarnings("unchecked")
    public boolean isEmpty() {
      List<String> logMessages = (List<String>) state.get("logMessages");
      Map<String, ?> patternGroupMap = (Map<String, ?>) state.get("patternGroupMap");
      return (logMessages == null || logMessages.isEmpty())
          && (patternGroupMap == null || patternGroupMap.isEmpty());
    }

    @Override
    public Object value(Object... argList) {
      // Return the current state for use by LogPatternAggFunction.result()
      // The argList contains [maxSampleCount, variableCountThreshold, thresholdPercentage,
      // showNumberedToken]
      if (isEmpty()) {
        return null;
      }
      int maxSampleCount =
          argList.length > 0 && argList[0] != null ? ((Number) argList[0]).intValue() : 10;
      int variableCountThreshold =
          argList.length > 1 && argList[1] != null
              ? ((Number) argList[1]).intValue()
              : BrainLogParser.DEFAULT_VARIABLE_COUNT_THRESHOLD;
      double thresholdPercentage =
          argList.length > 2 && argList[2] != null
              ? ((Number) argList[2]).doubleValue()
              : BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE;
      boolean showNumberedToken =
          argList.length > 3 && argList[3] != null && Boolean.TRUE.equals(argList[3]);

      return PatternAggregationHelpers.producePatternResult(
          state, maxSampleCount, variableCountThreshold, thresholdPercentage, showNumberedToken);
    }
  }
}
