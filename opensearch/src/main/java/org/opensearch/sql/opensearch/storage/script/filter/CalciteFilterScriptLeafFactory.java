/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.SOURCES;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite script leaf factory that produces script executor for each leaf. */
class CalciteFilterScriptLeafFactory implements FilterScript.LeafFactory {

  private final Function1<DataContext, Object[]> function;

  /** Parameters for the calcite script. */
  private final Map<String, Object> params;

  /** Document lookup that returns doc values. */
  private final SearchLookup lookup;

  /**
   * Stores the parameter names to the actual indices in SOURCES. Generate it in advance in factory
   * to save the process for each document*
   */
  private final Map<String, Integer> parametersToIndex;

  public CalciteFilterScriptLeafFactory(
      Function1<DataContext, Object[]> function, Map<String, Object> params, SearchLookup lookup) {
    this.function = function;
    this.params = params;
    this.lookup = lookup;
    this.parametersToIndex =
        IntStream.range(0, ((List<Integer>) params.get(SOURCES)).size())
            .boxed()
            .collect(Collectors.toMap(i -> "?" + i, i -> i));
  }

  @Override
  public FilterScript newInstance(LeafReaderContext ctx) {
    return new CalciteFilterScript(function, lookup, ctx, params, parametersToIndex);
  }
}
