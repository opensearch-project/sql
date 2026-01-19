/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Scripted metric UDAF implementation for the Pattern (BRAIN) aggregation function.
 *
 * <p>This implementation handles the pushdown of the pattern detection algorithm to OpenSearch,
 * using the BrainLogParser for log pattern analysis. The four script phases are:
 *
 * <ul>
 *   <li><b>init_script</b>: Initializes state with logMessages buffer and patternGroupMap
 *   <li><b>map_script</b>: Adds log messages to accumulator, triggers partial merge when buffer is
 *       full
 *   <li><b>combine_script</b>: Returns shard-level state for the reduce phase
 *   <li><b>reduce_script</b>: Combines all shard states and produces final pattern results
 * </ul>
 */
public class PatternScriptedMetricUDAF implements ScriptedMetricUDAF {

  // Default parameter values for pattern UDAF
  private static final int DEFAULT_MAX_SAMPLE_COUNT = 10;
  private static final int DEFAULT_BUFFER_LIMIT = 100000;
  private static final int DEFAULT_VARIABLE_COUNT_THRESHOLD = 5;
  private static final BigDecimal DEFAULT_THRESHOLD_PERCENTAGE = BigDecimal.valueOf(0.3);

  /** Singleton instance */
  public static final PatternScriptedMetricUDAF INSTANCE = new PatternScriptedMetricUDAF();

  private PatternScriptedMetricUDAF() {}

  @Override
  public BuiltinFunctionName getFunctionName() {
    return BuiltinFunctionName.INTERNAL_PATTERN;
  }

  @Override
  public RexNode buildInitScript(ScriptContext context) {
    RexBuilder rexBuilder = context.getRexBuilder();
    RexNode stateRef = context.addSpecialVariableRef("state", SqlTypeName.ANY);
    return rexBuilder.makeCall(PPLBuiltinOperators.PATTERN_INIT_UDF, List.of(stateRef));
  }

  @Override
  public RexNode buildMapScript(ScriptContext context, List<RexNode> args) {
    RexBuilder rexBuilder = context.getRexBuilder();
    List<RexNode> mapArgs = new ArrayList<>();

    // Add state variable reference
    RexNode stateRef = context.addSpecialVariableRef("state", SqlTypeName.ANY);
    mapArgs.add(stateRef);

    // Add field reference (first argument)
    if (!args.isEmpty()) {
      mapArgs.add(args.get(0));
    }

    // Add parameters with defaults:
    // args[1] = maxSampleCount
    // args[2] = bufferLimit
    // args[3] = showNumberedToken (not used in map script)
    // args[4] = thresholdPercentage (optional)
    // args[5] = variableCountThreshold (optional)
    mapArgs.add(getArgOrDefault(args, 1, makeIntLiteral(rexBuilder, DEFAULT_MAX_SAMPLE_COUNT)));
    mapArgs.add(getArgOrDefault(args, 2, makeIntLiteral(rexBuilder, DEFAULT_BUFFER_LIMIT)));
    mapArgs.add(
        getArgOrDefault(args, 5, makeIntLiteral(rexBuilder, DEFAULT_VARIABLE_COUNT_THRESHOLD)));
    mapArgs.add(
        getArgOrDefault(args, 4, makeDoubleLiteral(rexBuilder, DEFAULT_THRESHOLD_PERCENTAGE)));

    return rexBuilder.makeCall(PPLBuiltinOperators.PATTERN_ADD_UDF, mapArgs);
  }

  @Override
  public RexNode buildCombineScript(ScriptContext context) {
    // Combine script simply returns the shard-level state
    return context.addSpecialVariableRef("state", SqlTypeName.ANY);
  }

  @Override
  public RexNode buildReduceScript(ScriptContext context, List<RexNode> args) {
    RexBuilder rexBuilder = context.getRexBuilder();
    RexNode statesRef = context.addSpecialVariableRef("states", SqlTypeName.ANY);

    List<RexNode> reduceArgs = new ArrayList<>();
    reduceArgs.add(statesRef);

    // maxSampleCount
    reduceArgs.add(getArgOrDefault(args, 1, makeIntLiteral(rexBuilder, DEFAULT_MAX_SAMPLE_COUNT)));

    // Determine variableCountThreshold and thresholdPercentage
    RexNode variableCountThreshold = makeIntLiteral(rexBuilder, DEFAULT_VARIABLE_COUNT_THRESHOLD);
    RexNode thresholdPercentage = makeDoubleLiteral(rexBuilder, DEFAULT_THRESHOLD_PERCENTAGE);

    if (args.size() > 5) {
      thresholdPercentage = args.get(4);
      variableCountThreshold = args.get(5);
    } else if (args.size() > 4) {
      RexNode arg4 = args.get(4);
      SqlTypeName arg4Type = arg4.getType().getSqlTypeName();
      if (arg4Type == SqlTypeName.DOUBLE
          || arg4Type == SqlTypeName.DECIMAL
          || arg4Type == SqlTypeName.FLOAT) {
        thresholdPercentage = arg4;
      } else {
        variableCountThreshold = arg4;
      }
    }

    reduceArgs.add(variableCountThreshold);
    reduceArgs.add(thresholdPercentage);

    // showNumberedToken (default false)
    reduceArgs.add(getArgOrDefault(args, 3, rexBuilder.makeLiteral(false)));

    return rexBuilder.makeCall(PPLBuiltinOperators.PATTERN_RESULT_UDF, reduceArgs);
  }

  /** Get argument from list or return default value. */
  private static RexNode getArgOrDefault(List<RexNode> args, int index, RexNode defaultValue) {
    return args.size() > index ? args.get(index) : defaultValue;
  }

  /** Create integer literal for pattern UDAF parameters. */
  private static RexNode makeIntLiteral(RexBuilder rexBuilder, int value) {
    return rexBuilder.makeLiteral(
        value, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
  }

  /** Create double literal for pattern UDAF parameters. */
  private static RexNode makeDoubleLiteral(RexBuilder rexBuilder, BigDecimal value) {
    return rexBuilder.makeLiteral(
        value, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true);
  }
}
