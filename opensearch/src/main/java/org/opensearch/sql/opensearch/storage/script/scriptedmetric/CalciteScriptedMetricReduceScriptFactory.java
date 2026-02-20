/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.scriptedmetric;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.opensearch.script.ScriptedMetricAggContexts;

/**
 * Factory for Calcite-based ReduceScript in scripted metric aggregations. Produces final result
 * from all shard-level combined results using RexNode expressions.
 */
@RequiredArgsConstructor
public class CalciteScriptedMetricReduceScriptFactory
    implements ScriptedMetricAggContexts.ReduceScript.Factory {

  private final Function1<DataContext, Object[]> function;

  @Override
  public ScriptedMetricAggContexts.ReduceScript newInstance(
      Map<String, Object> params, List<Object> states) {
    return new CalciteScriptedMetricReduceScript(function, params, states);
  }

  /** ReduceScript that executes compiled RexNode expression. */
  private static class CalciteScriptedMetricReduceScript
      extends ScriptedMetricAggContexts.ReduceScript {

    private final Function1<DataContext, Object[]> function;

    public CalciteScriptedMetricReduceScript(
        Function1<DataContext, Object[]> function,
        Map<String, Object> params,
        List<Object> states) {
      super(params, states);
      this.function = function;
    }

    @Override
    public Object execute() {
      // Create data context for reduce script
      DataContext dataContext =
          new ScriptedMetricDataContext.ReduceContext(getParams(), getStates());

      // Execute the compiled RexNode expression
      Object[] result = function.apply(dataContext);

      // Return the final result
      return (result != null && result.length > 0) ? result[0] : getStates();
    }
  }
}
