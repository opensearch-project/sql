/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script;

import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rex.RexBuilder;
import org.opensearch.script.AggregationScript;
import org.opensearch.script.FilterScript;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.serde.DefaultExpressionSerializer;

/**
 * Custom expression script engine that supports using core engine expression code in DSL as a new
 * script language just like built-in Painless language.
 */
@RequiredArgsConstructor
public class CompoundedScriptEngine implements ScriptEngine {

  /** Expression script language name. */
  public static final String COMPOUNDED_LANG_NAME = "opensearch_compounded_script";

  public static final String ENGINE_TYPE = "engine_type";
  public static final String V2_ENGINE_TYPE = "v2";
  public static final String CALCITE_ENGINE_TYPE = "calcite";

  private static final ExpressionScriptEngine v2ExpressionScriptEngine =
      new ExpressionScriptEngine(new DefaultExpressionSerializer());

  private final CalciteScriptEngine calciteScriptEngine;

  public CompoundedScriptEngine() {
    RexBuilder rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    this.calciteScriptEngine = new CalciteScriptEngine(cluster);
  }

  @Override
  public String getType() {
    return COMPOUNDED_LANG_NAME;
  }

  @Override
  public <T> T compile(
      String scriptName, String scriptCode, ScriptContext<T> context, Map<String, String> options) {
    return switch (options.getOrDefault(ENGINE_TYPE, V2_ENGINE_TYPE)) {
      case CALCITE_ENGINE_TYPE -> calciteScriptEngine.compile(
          scriptName, scriptCode, context, options);
      case V2_ENGINE_TYPE -> v2ExpressionScriptEngine.compile(
          scriptName, scriptCode, context, options);
      default -> throw new IllegalStateException(
          "Unexpected engine type: " + options.getOrDefault(ENGINE_TYPE, V2_ENGINE_TYPE));
    };
  }

  @Override
  public Set<ScriptContext<?>> getSupportedContexts() {
    return Set.of(FilterScript.CONTEXT, AggregationScript.CONTEXT);
  }
}
