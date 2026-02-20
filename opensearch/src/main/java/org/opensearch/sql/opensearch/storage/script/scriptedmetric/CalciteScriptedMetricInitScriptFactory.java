/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.scriptedmetric;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.opensearch.script.ScriptedMetricAggContexts;

/**
 * Factory for Calcite-based InitScript in scripted metric aggregations. Executes RexNode
 * expressions compiled to Java code via CalciteScriptEngine.
 */
@RequiredArgsConstructor
public class CalciteScriptedMetricInitScriptFactory
    implements ScriptedMetricAggContexts.InitScript.Factory {

  private final Function1<DataContext, Object[]> function;

  @Override
  public ScriptedMetricAggContexts.InitScript newInstance(
      Map<String, Object> params, Map<String, Object> state) {
    return new CalciteScriptedMetricInitScript(function, params, state);
  }

  /** InitScript that executes compiled RexNode expression. */
  private static class CalciteScriptedMetricInitScript
      extends ScriptedMetricAggContexts.InitScript {

    private final Function1<DataContext, Object[]> function;

    public CalciteScriptedMetricInitScript(
        Function1<DataContext, Object[]> function,
        Map<String, Object> params,
        Map<String, Object> state) {
      super(params, state);
      this.function = function;
    }

    @Override
    public void execute() {
      // Create data context for init script (no document access, only params)
      @SuppressWarnings("unchecked")
      Map<String, Object> state = (Map<String, Object>) getState();
      DataContext dataContext = new ScriptedMetricDataContext.InitContext(getParams(), state);

      // Execute the compiled RexNode expression and merge result into state
      Object[] result = function.apply(dataContext);
      ScriptedMetricDataContext.mergeResultIntoState(result, state);
    }
  }
}
