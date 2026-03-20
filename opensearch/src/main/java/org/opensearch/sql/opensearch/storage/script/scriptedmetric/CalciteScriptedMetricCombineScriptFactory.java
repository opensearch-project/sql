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
 * Factory for Calcite-based CombineScript in scripted metric aggregations. Combines shard-level
 * accumulators using RexNode expressions.
 */
@RequiredArgsConstructor
public class CalciteScriptedMetricCombineScriptFactory
    implements ScriptedMetricAggContexts.CombineScript.Factory {

  private final Function1<DataContext, Object[]> function;

  @Override
  public ScriptedMetricAggContexts.CombineScript newInstance(
      Map<String, Object> params, Map<String, Object> state) {
    return new CalciteScriptedMetricCombineScript(function, params, state);
  }

  /** CombineScript that executes compiled RexNode expression. */
  private static class CalciteScriptedMetricCombineScript
      extends ScriptedMetricAggContexts.CombineScript {

    private final Function1<DataContext, Object[]> function;

    public CalciteScriptedMetricCombineScript(
        Function1<DataContext, Object[]> function,
        Map<String, Object> params,
        Map<String, Object> state) {
      super(params, state);
      this.function = function;
    }

    @Override
    public Object execute() {
      // Create data context for combine script
      @SuppressWarnings("unchecked")
      Map<String, Object> state = (Map<String, Object>) getState();
      DataContext dataContext = new ScriptedMetricDataContext.CombineContext(getParams(), state);

      // Execute the compiled RexNode expression
      Object[] result = function.apply(dataContext);

      // Return the combined result
      return (result != null && result.length > 0) ? result[0] : getState();
    }
  }
}
