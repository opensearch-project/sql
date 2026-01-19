/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.calcite.udf.udaf.LogPatternAggFunction.LogParserAccumulator;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.common.patterns.PatternUtils;

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
    if (acc.size() == 0 && acc.logSize() == 0) {
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
    this.bufferLimit = bufferLimit;
    this.maxSampleCount = maxSampleCount;
    this.showNumberedToken = showNumberedToken;
    this.variableCountThreshold = variableCountThreshold;
    this.thresholdPercentage = thresholdPercentage;
    acc.evaluate(field);
    if (bufferLimit > 0 && acc.logSize() == bufferLimit) {
      acc.partialMerge(
          maxSampleCount, variableCountThreshold, thresholdPercentage, showNumberedToken);
      acc.clearBuffer();
    }
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

  public static class LogParserAccumulator implements Accumulator {
    private final List<String> logMessages;
    public Map<String, Map<String, Object>> patternGroupMap = new HashMap<>();

    public int size() {
      return patternGroupMap.size();
    }

    public int logSize() {
      return logMessages.size();
    }

    public LogParserAccumulator() {
      this.logMessages = new ArrayList<>();
    }

    public void evaluate(String value) {
      logMessages.add(value);
    }

    public void clearBuffer() {
      logMessages.clear();
    }

    public void partialMerge(Object... argList) {
      if (logMessages.isEmpty()) {
        return;
      }
      assert argList.length == 4 : "partialMerge of LogParserAccumulator requires 4 parameters";
      int maxSampleCount = (int) argList[0];
      BrainLogParser logParser =
          new BrainLogParser((int) argList[1], ((Double) argList[2]).floatValue());
      Map<String, Map<String, Object>> partialPatternGroupMap =
          logParser.parseAllLogPatterns(logMessages, maxSampleCount);
      patternGroupMap =
          PatternUtils.mergePatternGroups(patternGroupMap, partialPatternGroupMap, maxSampleCount);
    }

    @Override
    public Object value(Object... argList) {
      partialMerge(argList);
      clearBuffer();

      return patternGroupMap.values().stream()
          .sorted(
              Comparator.comparing(
                  m -> (Long) m.get(PatternUtils.PATTERN_COUNT),
                  Comparator.nullsLast(Comparator.reverseOrder())))
          .map(
              m -> {
                String pattern = (String) m.get(PatternUtils.PATTERN);
                Long count = (Long) m.get(PatternUtils.PATTERN_COUNT);
                List<String> sampleLogs = (List<String>) m.get(PatternUtils.SAMPLE_LOGS);
                // For aggregation mode, always return pattern with wildcards (<*>, <*IP*>).
                // The transformation to numbered tokens (<token1>, <token2>) and token
                // extraction is done downstream by evalAggSamples in flattenParsedPattern.
                // This ensures consistent behavior between UDAF pushdown and regular
                // aggregation paths.
                return ImmutableMap.of(
                    PatternUtils.PATTERN,
                    pattern, // Always return original wildcard format
                    PatternUtils.PATTERN_COUNT,
                    count,
                    PatternUtils.TOKENS,
                    Collections.EMPTY_MAP, // Tokens computed downstream by evalAggSamples
                    PatternUtils.SAMPLE_LOGS,
                    sampleLogs);
              })
          .collect(Collectors.toList());
    }
  }
}
