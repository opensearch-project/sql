/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.scriptedmetric;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.ScriptedMetricAggContexts;
import org.opensearch.search.lookup.SearchLookup;

/**
 * Factory for Calcite-based MapScript in scripted metric aggregations. Executes RexNode expressions
 * compiled to Java code with document field access.
 */
@RequiredArgsConstructor
public class CalciteScriptedMetricMapScriptFactory
    implements ScriptedMetricAggContexts.MapScript.Factory {

  private final Function1<DataContext, Object[]> function;

  @Override
  public ScriptedMetricAggContexts.MapScript.LeafFactory newFactory(
      Map<String, Object> params, Map<String, Object> state, SearchLookup lookup) {
    return new CalciteMapScriptLeafFactory(function, params, state, lookup);
  }

  /** Leaf factory that creates MapScript instances for each segment. */
  @RequiredArgsConstructor
  private static class CalciteMapScriptLeafFactory
      implements ScriptedMetricAggContexts.MapScript.LeafFactory {

    private final Function1<DataContext, Object[]> function;
    private final Map<String, Object> params;
    private final Map<String, Object> state;
    private final SearchLookup lookup;

    @Override
    public ScriptedMetricAggContexts.MapScript newInstance(LeafReaderContext ctx) {
      return new CalciteScriptedMetricMapScript(function, params, state, lookup, ctx);
    }
  }

  /**
   * MapScript that executes compiled RexNode expression for each document.
   *
   * <p>The DataContext is created once in the constructor and reused for all documents to avoid
   * object allocation overhead per document. This is safe because:
   *
   * <ul>
   *   <li>params, state references don't change between documents
   *   <li>doc and sourceLookup are updated internally by OpenSearch before each execute() call
   *   <li>sources and digests (derived from params) are the same for all documents
   * </ul>
   */
  private static class CalciteScriptedMetricMapScript extends ScriptedMetricAggContexts.MapScript {

    private final Function1<DataContext, Object[]> function;
    private final DataContext dataContext;

    public CalciteScriptedMetricMapScript(
        Function1<DataContext, Object[]> function,
        Map<String, Object> params,
        Map<String, Object> state,
        SearchLookup lookup,
        LeafReaderContext leafContext) {
      super(params, state, lookup, leafContext);
      this.function = function;
      // Create DataContext once and reuse for all documents in this segment.
      // OpenSearch updates doc values and source lookup internally before each execute().
      this.dataContext =
          new ScriptedMetricDataContext.MapContext(
              params, state, getDoc(), lookup.getLeafSearchLookup(leafContext).source());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute() {
      // Execute the compiled RexNode expression (reusing the same DataContext)
      Object[] result = function.apply(dataContext);
      ScriptedMetricDataContext.mergeResultIntoState(result, (Map<String, Object>) getState());
    }
  }
}
