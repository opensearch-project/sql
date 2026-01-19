/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.patterns;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Static helper methods for pattern aggregation operations. These methods wrap the complex logic in
 * BrainLogParser and PatternUtils to be callable from UDFs in scripted metric aggregations.
 */
public final class PatternAggregationHelpers {

  private PatternAggregationHelpers() {
    // Utility class
  }

  /**
   * Initialize pattern accumulator state.
   *
   * @return Empty accumulator map with logMessages buffer and patternGroupMap
   */
  public static Map<String, Object> initPatternAccumulator() {
    Map<String, Object> acc = new HashMap<>();
    acc.put("logMessages", new ArrayList<String>());
    acc.put("patternGroupMap", new HashMap<String, Map<String, Object>>());
    return acc;
  }

  /**
   * Initialize pattern accumulator state in-place. This method is designed for OpenSearch scripted
   * metric aggregation's init_script phase, where the state map is provided by OpenSearch and must
   * be modified in-place rather than replaced.
   *
   * @param state The mutable state map provided by OpenSearch (will be modified in-place)
   * @return The same state map (for chaining/return value)
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> initPatternState(Object state) {
    Map<String, Object> stateMap = (Map<String, Object>) state;
    stateMap.put("logMessages", new ArrayList<String>());
    stateMap.put("patternGroupMap", new HashMap<String, Map<String, Object>>());
    return stateMap;
  }

  /**
   * Add a log message to the accumulator (overload for Object acc and int thresholdPercentage).
   * This overload handles the case when the accumulator is passed as a generic Object and
   * thresholdPercentage is passed as an integer at runtime (from the script engine).
   *
   * @param acc Current accumulator state (as Object, will be cast to Map)
   * @param logMessage The log message to process
   * @param maxSampleCount Maximum samples to keep per pattern
   * @param bufferLimit Maximum buffer size before triggering partial merge
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as int)
   * @return Updated accumulator
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> addLogToPattern(
      Object acc,
      String logMessage,
      int maxSampleCount,
      int bufferLimit,
      int variableCountThreshold,
      int thresholdPercentage) {
    return addLogToPattern(
        (Map<String, Object>) acc,
        logMessage,
        maxSampleCount,
        bufferLimit,
        variableCountThreshold,
        (double) thresholdPercentage);
  }

  /**
   * Add a log message to the accumulator (overload for Object acc and BigDecimal
   * thresholdPercentage). This overload handles the case when the accumulator is passed as a
   * generic Object and thresholdPercentage is passed as BigDecimal at runtime.
   *
   * @param acc Current accumulator state (as Object, will be cast to Map)
   * @param logMessage The log message to process
   * @param maxSampleCount Maximum samples to keep per pattern
   * @param bufferLimit Maximum buffer size before triggering partial merge
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as BigDecimal)
   * @return Updated accumulator
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> addLogToPattern(
      Object acc,
      String logMessage,
      int maxSampleCount,
      int bufferLimit,
      int variableCountThreshold,
      java.math.BigDecimal thresholdPercentage) {
    return addLogToPattern(
        (Map<String, Object>) acc,
        logMessage,
        maxSampleCount,
        bufferLimit,
        variableCountThreshold,
        thresholdPercentage != null
            ? thresholdPercentage.doubleValue()
            : BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE);
  }

  /**
   * Add a log message to the accumulator (overload for Object acc and double thresholdPercentage).
   * This overload handles the case when the accumulator is passed as a generic Object and
   * thresholdPercentage is passed as double at runtime.
   *
   * @param acc Current accumulator state (as Object, will be cast to Map)
   * @param logMessage The log message to process
   * @param maxSampleCount Maximum samples to keep per pattern
   * @param bufferLimit Maximum buffer size before triggering partial merge
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as double)
   * @return Updated accumulator
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> addLogToPattern(
      Object acc,
      String logMessage,
      int maxSampleCount,
      int bufferLimit,
      int variableCountThreshold,
      double thresholdPercentage) {
    return addLogToPattern(
        (Map<String, Object>) acc,
        logMessage,
        maxSampleCount,
        bufferLimit,
        variableCountThreshold,
        thresholdPercentage);
  }

  /**
   * Add a log message to the accumulator (overload for int thresholdPercentage). This overload
   * handles the case when thresholdPercentage is passed as an integer at runtime.
   *
   * @param acc Current accumulator state
   * @param logMessage The log message to process
   * @param maxSampleCount Maximum samples to keep per pattern
   * @param bufferLimit Maximum buffer size before triggering partial merge
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as int)
   * @return Updated accumulator
   */
  public static Map<String, Object> addLogToPattern(
      Map<String, Object> acc,
      String logMessage,
      int maxSampleCount,
      int bufferLimit,
      int variableCountThreshold,
      int thresholdPercentage) {
    return addLogToPattern(
        acc,
        logMessage,
        maxSampleCount,
        bufferLimit,
        variableCountThreshold,
        (double) thresholdPercentage);
  }

  /**
   * Add a log message to the accumulator (overload for BigDecimal thresholdPercentage). This
   * overload handles the case when thresholdPercentage is passed as BigDecimal at runtime.
   *
   * @param acc Current accumulator state
   * @param logMessage The log message to process
   * @param maxSampleCount Maximum samples to keep per pattern
   * @param bufferLimit Maximum buffer size before triggering partial merge
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as BigDecimal)
   * @return Updated accumulator
   */
  public static Map<String, Object> addLogToPattern(
      Map<String, Object> acc,
      String logMessage,
      int maxSampleCount,
      int bufferLimit,
      int variableCountThreshold,
      java.math.BigDecimal thresholdPercentage) {
    return addLogToPattern(
        acc,
        logMessage,
        maxSampleCount,
        bufferLimit,
        variableCountThreshold,
        thresholdPercentage != null
            ? thresholdPercentage.doubleValue()
            : BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE);
  }

  /**
   * Add a log message to the accumulator and trigger partial merge if buffer is full.
   *
   * @param acc Current accumulator state
   * @param logMessage The log message to process
   * @param maxSampleCount Maximum samples to keep per pattern
   * @param bufferLimit Maximum buffer size before triggering partial merge
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage
   * @return Updated accumulator
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> addLogToPattern(
      Map<String, Object> acc,
      String logMessage,
      int maxSampleCount,
      int bufferLimit,
      int variableCountThreshold,
      double thresholdPercentage) {

    if (logMessage == null) {
      return acc;
    }

    List<String> logMessages = (List<String>) acc.get("logMessages");
    logMessages.add(logMessage);

    // Trigger partial merge when buffer reaches limit
    if (bufferLimit > 0 && logMessages.size() >= bufferLimit) {
      Map<String, Map<String, Object>> patternGroupMap =
          (Map<String, Map<String, Object>>) acc.get("patternGroupMap");

      BrainLogParser parser =
          new BrainLogParser(variableCountThreshold, (float) thresholdPercentage);
      Map<String, Map<String, Object>> partialPatterns =
          parser.parseAllLogPatterns(logMessages, maxSampleCount);

      patternGroupMap =
          PatternUtils.mergePatternGroups(patternGroupMap, partialPatterns, maxSampleCount);

      acc.put("patternGroupMap", patternGroupMap);
      logMessages.clear();
    }

    return acc;
  }

  /**
   * Combine two accumulators (for combine_script phase).
   *
   * @param acc1 First accumulator
   * @param acc2 Second accumulator
   * @param maxSampleCount Maximum samples to keep per pattern
   * @return Merged accumulator
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> combinePatternAccumulators(
      Map<String, Object> acc1, Map<String, Object> acc2, int maxSampleCount) {

    Map<String, Map<String, Object>> patterns1 =
        (Map<String, Map<String, Object>>) acc1.get("patternGroupMap");
    Map<String, Map<String, Object>> patterns2 =
        (Map<String, Map<String, Object>>) acc2.get("patternGroupMap");

    Map<String, Map<String, Object>> merged =
        PatternUtils.mergePatternGroups(patterns1, patterns2, maxSampleCount);

    Map<String, Object> result = new HashMap<>();
    result.put("logMessages", new ArrayList<>());
    result.put("patternGroupMap", merged);
    return result;
  }

  /**
   * Produce final pattern result (for reduce_script phase).
   *
   * @param acc Accumulator state
   * @param maxSampleCount Maximum samples per pattern
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage
   * @param showNumberedToken Whether to show numbered tokens in output
   * @return List of pattern result objects sorted by count
   */
  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> producePatternResult(
      Map<String, Object> acc,
      int maxSampleCount,
      int variableCountThreshold,
      double thresholdPercentage,
      boolean showNumberedToken) {

    // Process any remaining logs in buffer
    List<String> logMessages = (List<String>) acc.get("logMessages");
    Map<String, Map<String, Object>> patternGroupMap =
        (Map<String, Map<String, Object>>) acc.get("patternGroupMap");

    if (logMessages != null && !logMessages.isEmpty()) {
      BrainLogParser parser =
          new BrainLogParser(variableCountThreshold, (float) thresholdPercentage);
      Map<String, Map<String, Object>> partialPatterns =
          parser.parseAllLogPatterns(logMessages, maxSampleCount);
      patternGroupMap =
          PatternUtils.mergePatternGroups(patternGroupMap, partialPatterns, maxSampleCount);
    }

    // Format and sort final output by pattern count
    return patternGroupMap.values().stream()
        .sorted(
            Comparator.comparing(
                m -> (Long) m.get(PatternUtils.PATTERN_COUNT),
                Comparator.nullsLast(Comparator.reverseOrder())))
        .map(m -> formatPatternOutput(m, showNumberedToken))
        .collect(Collectors.toList());
  }

  /**
   * Produce final pattern result from states array (overload for int thresholdPercentage). This
   * overload handles the case when thresholdPercentage is passed as an integer at runtime.
   *
   * @param states List of shard-level accumulator states
   * @param maxSampleCount Maximum samples per pattern
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as int)
   * @param showNumberedToken Whether to show numbered tokens in output
   * @return List of pattern result objects sorted by count
   */
  public static List<Map<String, Object>> producePatternResultFromStates(
      List<Object> states,
      int maxSampleCount,
      int variableCountThreshold,
      int thresholdPercentage,
      boolean showNumberedToken) {
    return producePatternResultFromStates(
        states,
        maxSampleCount,
        variableCountThreshold,
        (double) thresholdPercentage,
        showNumberedToken);
  }

  /**
   * Produce final pattern result from states array (overload for Object states). This overload
   * handles the case when states is passed as a generic Object at runtime due to type erasure.
   *
   * @param states List of shard-level accumulator states (as Object, will be cast to List)
   * @param maxSampleCount Maximum samples per pattern
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage
   * @param showNumberedToken Whether to show numbered tokens in output
   * @return List of pattern result objects sorted by count
   */
  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> producePatternResultFromStates(
      Object states,
      int maxSampleCount,
      int variableCountThreshold,
      double thresholdPercentage,
      boolean showNumberedToken) {
    return producePatternResultFromStates(
        (List<Object>) states,
        maxSampleCount,
        variableCountThreshold,
        thresholdPercentage,
        showNumberedToken);
  }

  /**
   * Produce final pattern result from states array (overload for Object states with BigDecimal).
   * This overload handles the case when states is Object and thresholdPercentage is BigDecimal.
   *
   * @param states List of shard-level accumulator states (as Object, will be cast to List)
   * @param maxSampleCount Maximum samples per pattern
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as BigDecimal)
   * @param showNumberedToken Whether to show numbered tokens in output
   * @return List of pattern result objects sorted by count
   */
  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> producePatternResultFromStates(
      Object states,
      int maxSampleCount,
      int variableCountThreshold,
      java.math.BigDecimal thresholdPercentage,
      boolean showNumberedToken) {
    return producePatternResultFromStates(
        (List<Object>) states,
        maxSampleCount,
        variableCountThreshold,
        thresholdPercentage != null
            ? thresholdPercentage.doubleValue()
            : BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE,
        showNumberedToken);
  }

  /**
   * Produce final pattern result from states array (overload for BigDecimal thresholdPercentage).
   * This overload handles the case when thresholdPercentage is passed as BigDecimal at runtime.
   *
   * @param states List of shard-level accumulator states
   * @param maxSampleCount Maximum samples per pattern
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage (as BigDecimal)
   * @param showNumberedToken Whether to show numbered tokens in output
   * @return List of pattern result objects sorted by count
   */
  public static List<Map<String, Object>> producePatternResultFromStates(
      List<Object> states,
      int maxSampleCount,
      int variableCountThreshold,
      java.math.BigDecimal thresholdPercentage,
      boolean showNumberedToken) {
    return producePatternResultFromStates(
        states,
        maxSampleCount,
        variableCountThreshold,
        thresholdPercentage != null
            ? thresholdPercentage.doubleValue()
            : BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE,
        showNumberedToken);
  }

  /**
   * Produce final pattern result from states array (for reduce_script phase). This method combines
   * all shard-level states and produces the final aggregated result.
   *
   * @param states List of shard-level accumulator states
   * @param maxSampleCount Maximum samples per pattern
   * @param variableCountThreshold Brain parser variable count threshold
   * @param thresholdPercentage Brain parser frequency threshold percentage
   * @param showNumberedToken Whether to show numbered tokens in output
   * @return List of pattern result objects sorted by count
   */
  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> producePatternResultFromStates(
      List<Object> states,
      int maxSampleCount,
      int variableCountThreshold,
      double thresholdPercentage,
      boolean showNumberedToken) {

    if (states == null || states.isEmpty()) {
      return new ArrayList<>();
    }

    // Combine all states into a single accumulator
    Map<String, Object> combined = (Map<String, Object>) states.get(0);
    for (int i = 1; i < states.size(); i++) {
      Map<String, Object> state = (Map<String, Object>) states.get(i);
      combined = combinePatternAccumulators(combined, state, maxSampleCount);
    }

    // Produce final result from combined state
    return producePatternResult(
        combined, maxSampleCount, variableCountThreshold, thresholdPercentage, showNumberedToken);
  }

  /**
   * Format a single pattern result for output.
   *
   * <p>Note: Token extraction is NOT done here. The pattern is returned with wildcards (e.g.,
   * {@code <*>}) and token extraction is performed later by {@code
   * PatternParserFunctionImpl.evalAggSamples()} after the data returns from OpenSearch. This
   * approach avoids the XContent serialization issue where nested {@code Map<String, List<String>>}
   * structures are not properly serialized.
   *
   * @param patternInfo Pattern information map
   * @param showNumberedToken Whether numbered tokens should be shown (determines output format)
   * @return Formatted pattern output with pattern, count, and sample_logs
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> formatPatternOutput(
      Map<String, Object> patternInfo, boolean showNumberedToken) {

    String pattern = (String) patternInfo.get(PatternUtils.PATTERN);
    Long count = (Long) patternInfo.get(PatternUtils.PATTERN_COUNT);
    List<String> sampleLogs = (List<String>) patternInfo.get(PatternUtils.SAMPLE_LOGS);

    // For UDAF pushdown, we don't compute tokens here.
    // Tokens will be computed by PatternParserFunctionImpl.evalAggSamples() after data returns
    // from OpenSearch. This avoids XContent serialization issues with nested Map structures.
    // The showNumberedToken flag is passed through to indicate the expected output format.
    return ImmutableMap.of(
        PatternUtils.PATTERN,
        pattern,
        PatternUtils.PATTERN_COUNT,
        count,
        PatternUtils.SAMPLE_LOGS,
        sampleLogs);
  }
}
