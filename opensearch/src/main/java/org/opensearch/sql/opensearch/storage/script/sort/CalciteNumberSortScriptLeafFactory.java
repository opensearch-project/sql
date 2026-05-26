/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.sort;

import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.SOURCES;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.NumberSortScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite number sort script leaf factory that produces script executor for each leaf. */
@EqualsAndHashCode(callSuper = false)
class CalciteNumberSortScriptLeafFactory implements NumberSortScript.LeafFactory {

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

  public CalciteNumberSortScriptLeafFactory(
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
  public NumberSortScript newInstance(LeafReaderContext context) throws IOException {
    return new CalciteNumberSortScript(function, lookup, context, params, parametersToIndex);
  }

  @Override
  public boolean needs_score() {
    return false;
  }
}
