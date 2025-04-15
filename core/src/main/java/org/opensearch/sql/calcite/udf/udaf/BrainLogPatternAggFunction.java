/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.patterns.BrainLogParser;

public class BrainLogPatternAggFunction
    implements UserDefinedAggFunction<BrainLogPatternAggFunction.BrainLogParserAccumulator> {
  private int variableCountThreshold = BrainLogParser.DEFAULT_VARIABLE_COUNT_THRESHOLD;
  private double thresholdPercentage = BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE;

  @Override
  public BrainLogParserAccumulator init() {
    return new BrainLogParserAccumulator();
  }

  @Override
  public Object result(BrainLogParserAccumulator acc) {
    if (acc.size() == 0) {
      return null;
    }

    return acc.value(variableCountThreshold, thresholdPercentage);
  }

  @Override
  public BrainLogParserAccumulator add(BrainLogParserAccumulator acc, Object... values) {
    throw new SyntaxCheckException(
        "Unsupported function signature for brain aggregate. Valid parameters include (field:"
            + " required string), [variable_count_threshold: optional integer],"
            + " [frequency_threshold_percentage: optional double]");
  }

  public BrainLogParserAccumulator add(
      BrainLogParserAccumulator acc,
      String field,
      double thresholdPercentage,
      int variableCountThreshold) {
    if (Objects.isNull(field)) {
      return acc;
    }
    this.variableCountThreshold = variableCountThreshold;
    this.thresholdPercentage = thresholdPercentage;
    acc.evaluate(field);
    return acc;
  }

  public BrainLogParserAccumulator add(
      BrainLogParserAccumulator acc, String field, int variableCountThreshold) {
    return add(acc, field, variableCountThreshold, this.thresholdPercentage);
  }

  public BrainLogParserAccumulator add(
      BrainLogParserAccumulator acc, String field, double thresholdPercentage) {
    return add(acc, field, this.variableCountThreshold, thresholdPercentage);
  }

  public BrainLogParserAccumulator add(BrainLogParserAccumulator acc, String field) {
    return add(acc, field, this.thresholdPercentage, this.variableCountThreshold);
  }

  public static class BrainLogParserAccumulator implements Accumulator {
    private final List<String> candidate;
    private BrainLogParser logParser;
    private List<List<String>> preprocessedLogs = new ArrayList<>();
    public BrainLogParser.GroupTokenAggregationInfo aggInfo =
        new BrainLogParser.GroupTokenAggregationInfo();

    public int size() {
      return candidate.size();
    }

    public BrainLogParserAccumulator() {
      this.candidate = new ArrayList<>();
    }

    public void evaluate(String value) {
      candidate.add(value);
    }

    @Override
    public Object value(Object... argList) {
      if (preprocessedLogs.isEmpty()) {
        this.logParser = new BrainLogParser((int) argList[0], ((Double) argList[1]).floatValue());
        Map<String, List<String>> messageToProcessedLogMap = new HashMap<>();
        preprocessedLogs = logParser.preprocessAllLogs(candidate);
        for (int i = 0; i < candidate.size(); i++) {
          messageToProcessedLogMap.put(candidate.get(i), preprocessedLogs.get(i));
        }
        aggInfo.setMessageToProcessedLogMap(messageToProcessedLogMap);
        aggInfo.setTokenFreqMap(logParser.getTokenFreqMap());
        aggInfo.setGroupTokenSetMap(logParser.getGroupTokenSetMap());
        aggInfo.setLogIdGroupCandidateMap(logParser.getLogIdGroupCandidateMap());
        aggInfo.setVariableCountThreshold(logParser.getVariableCountThreshold());
      }
      return aggInfo;
    }
  }
}
