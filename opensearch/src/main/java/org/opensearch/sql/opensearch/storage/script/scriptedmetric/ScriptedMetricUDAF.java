/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.scriptedmetric;

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.ScriptedMetricParser;
import org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine;
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer;
import org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper;
import org.opensearch.sql.opensearch.storage.serde.SerializationWrapper;

/**
 * Interface for User-Defined Aggregate Functions (UDAFs) that can be pushed down to OpenSearch as
 * scripted metric aggregations.
 *
 * <p>A scripted metric aggregation has four phases:
 *
 * <ul>
 *   <li><b>init_script</b>: Initializes the accumulator state on each shard
 *   <li><b>map_script</b>: Processes each document, updating the accumulator
 *   <li><b>combine_script</b>: Combines shard-level states (runs on each shard)
 *   <li><b>reduce_script</b>: Produces final result from all shard states (runs on coordinator)
 * </ul>
 *
 * <p>Implementations should encapsulate all domain-specific logic for a particular UDAF, keeping
 * the AggregateAnalyzer generic and reusable.
 */
public interface ScriptedMetricUDAF {

  /**
   * Returns the function name this UDAF handles.
   *
   * @return The BuiltinFunctionName that this UDAF implements
   */
  BuiltinFunctionName getFunctionName();

  /**
   * Build the init_script RexNode for initializing accumulator state.
   *
   * @param context The script context containing builders and utilities
   * @return RexNode representing the init script expression
   */
  RexNode buildInitScript(ScriptContext context);

  /**
   * Build the map_script RexNode for processing each document.
   *
   * @param context The script context containing builders and utilities
   * @param args The arguments from the aggregate call
   * @return RexNode representing the map script expression
   */
  RexNode buildMapScript(ScriptContext context, List<RexNode> args);

  /**
   * Build the combine_script RexNode for combining shard-level states.
   *
   * @param context The script context containing builders and utilities
   * @return RexNode representing the combine script expression
   */
  RexNode buildCombineScript(ScriptContext context);

  /**
   * Build the reduce_script RexNode for producing final result.
   *
   * @param context The script context containing builders and utilities
   * @param args The arguments from the aggregate call
   * @return RexNode representing the reduce script expression
   */
  RexNode buildReduceScript(ScriptContext context, List<RexNode> args);

  /**
   * Context object providing utilities for script generation. Each script phase gets its own
   * context with isolated parameter helpers.
   */
  class ScriptContext {
    private final RexBuilder rexBuilder;
    private final ScriptParameterHelper paramHelper;
    private final RelOptCluster cluster;
    private final RelDataType rowType;
    private final Map<String, ExprType> fieldTypes;

    public ScriptContext(
        RexBuilder rexBuilder,
        ScriptParameterHelper paramHelper,
        RelOptCluster cluster,
        RelDataType rowType,
        Map<String, ExprType> fieldTypes) {
      this.rexBuilder = rexBuilder;
      this.paramHelper = paramHelper;
      this.cluster = cluster;
      this.rowType = rowType;
      this.fieldTypes = fieldTypes;
    }

    public RexBuilder getRexBuilder() {
      return rexBuilder;
    }

    public ScriptParameterHelper getParamHelper() {
      return paramHelper;
    }

    public RelOptCluster getCluster() {
      return cluster;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public Map<String, ExprType> getFieldTypes() {
      return fieldTypes;
    }

    /**
     * Add a special variable (like 'state' or 'states') and return its dynamic param reference.
     *
     * @param varName The variable name
     * @param type The SQL type for the parameter
     * @return RexNode representing the dynamic parameter reference
     */
    public RexNode addSpecialVariableRef(String varName, SqlTypeName type) {
      int index = paramHelper.addSpecialVariable(varName);
      return rexBuilder.makeDynamicParam(rexBuilder.getTypeFactory().createSqlType(type), index);
    }
  }

  /**
   * Build the complete scripted metric aggregation.
   *
   * <p>This is the main entry point that creates all four scripts and assembles them into an
   * OpenSearch aggregation builder. The default implementation handles the common boilerplate.
   *
   * @param args The arguments from the aggregate call
   * @param aggName The name of the aggregation
   * @param cluster The RelOptCluster for creating builders
   * @param rowType The row type containing field information
   * @param fieldTypes Map of field names to expression types
   * @return Pair of aggregation builder and metric parser
   */
  default Pair<AggregationBuilder, MetricParser> buildAggregation(
      List<Pair<RexNode, String>> args,
      String aggName,
      RelOptCluster cluster,
      RelDataType rowType,
      Map<String, ExprType> fieldTypes) {

    RelJsonSerializer serializer = new RelJsonSerializer(cluster);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    List<RelDataTypeField> fieldList = rowType.getFieldList();

    // Create parameter helpers for each script phase
    ScriptParameterHelper initParamHelper =
        new ScriptParameterHelper(fieldList, fieldTypes, rexBuilder);
    ScriptParameterHelper mapParamHelper =
        new ScriptParameterHelper(fieldList, fieldTypes, rexBuilder);
    ScriptParameterHelper combineParamHelper =
        new ScriptParameterHelper(fieldList, fieldTypes, rexBuilder);
    ScriptParameterHelper reduceParamHelper =
        new ScriptParameterHelper(fieldList, fieldTypes, rexBuilder);

    // Create contexts for each phase
    ScriptContext initContext =
        new ScriptContext(rexBuilder, initParamHelper, cluster, rowType, fieldTypes);
    ScriptContext mapContext =
        new ScriptContext(rexBuilder, mapParamHelper, cluster, rowType, fieldTypes);
    ScriptContext combineContext =
        new ScriptContext(rexBuilder, combineParamHelper, cluster, rowType, fieldTypes);
    ScriptContext reduceContext =
        new ScriptContext(rexBuilder, reduceParamHelper, cluster, rowType, fieldTypes);

    // Extract RexNodes from args
    List<RexNode> argRefs = args.stream().map(Pair::getKey).toList();

    // Build scripts
    RexNode initRex = buildInitScript(initContext);
    RexNode mapRex = buildMapScript(mapContext, argRefs);
    RexNode combineRex = buildCombineScript(combineContext);
    RexNode reduceRex = buildReduceScript(reduceContext, argRefs);

    // Create Script objects
    Script initScript = createScript(serializer, initRex, initParamHelper);
    Script mapScript = createScript(serializer, mapRex, mapParamHelper);
    Script combineScript = createScript(serializer, combineRex, combineParamHelper);
    Script reduceScript = createScript(serializer, reduceRex, reduceParamHelper);

    // Build scripted metric aggregation
    AggregationBuilder aggBuilder =
        AggregationBuilders.scriptedMetric(aggName)
            .initScript(initScript)
            .mapScript(mapScript)
            .combineScript(combineScript)
            .reduceScript(reduceScript);

    return Pair.of(aggBuilder, new ScriptedMetricParser(aggName));
  }

  /**
   * Create a Script object from a RexNode expression.
   *
   * @param serializer The JSON serializer for RexNode
   * @param rexNode The expression to serialize
   * @param paramHelper The parameter helper containing script parameters
   * @return Script object ready for OpenSearch
   */
  private static Script createScript(
      RelJsonSerializer serializer, RexNode rexNode, ScriptParameterHelper paramHelper) {
    String serializedCode = serializer.serialize(rexNode, paramHelper);
    String wrappedCode =
        SerializationWrapper.wrapWithLangType(
            CompoundedScriptEngine.ScriptEngineType.CALCITE, serializedCode);
    return new Script(
        Script.DEFAULT_SCRIPT_TYPE,
        CompoundedScriptEngine.COMPOUNDED_LANG_NAME,
        wrappedCode,
        paramHelper.getParameters());
  }
}
