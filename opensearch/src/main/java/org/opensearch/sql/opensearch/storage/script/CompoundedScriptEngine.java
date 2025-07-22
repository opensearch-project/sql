/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
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
import org.opensearch.sql.opensearch.storage.serde.SerializationWrapper;
import org.opensearch.sql.opensearch.storage.serde.SerializationWrapper.LangScriptWrapper;

/**
 * Custom expression script engine that supports using core engine expression code in DSL as a new
 * script language just like built-in Painless language.
 */
@RequiredArgsConstructor
public class CompoundedScriptEngine implements ScriptEngine {

  /** Expression script language name. */
  public static final String COMPOUNDED_LANG_NAME = "opensearch_compounded_script";

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
    LangScriptWrapper unwrapped = SerializationWrapper.unwrapLangType(scriptCode);
    T result;
    switch (unwrapped.langType) {
      case CALCITE:
        result = calciteScriptEngine.compile(scriptName, unwrapped.script, context, options);
        break;
      case V2:
        result = v2ExpressionScriptEngine.compile(scriptName, unwrapped.script, context, options);
        break;
      default:
        throw new IllegalArgumentException("Unsupported lang type: " + unwrapped.langType);
    }
    return result;
  }

  @Override
  public Set<ScriptContext<?>> getSupportedContexts() {
    return Set.of(FilterScript.CONTEXT, AggregationScript.CONTEXT);
  }

  public enum ScriptEngineType {
    V2("v2"),
    CALCITE("calcite");

    private final String type;

    ScriptEngineType(String type) {
      this.type = type;
    }

    public static ScriptEngineType fromString(String value) {
      for (ScriptEngineType engineType : ScriptEngineType.values()) {
        if (engineType.type.equalsIgnoreCase(value)) {
          return engineType;
        }
      }
      throw new IllegalArgumentException("Unknown script engine type: " + value);
    }

    @JsonCreator
    public static ScriptEngineType fromJson(String value) {
      return fromString(value);
    }

    @JsonValue
    public String toJson() {
      return type;
    }
  }
}
