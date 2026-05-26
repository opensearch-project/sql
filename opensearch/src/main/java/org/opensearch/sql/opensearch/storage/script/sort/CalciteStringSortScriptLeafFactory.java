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
import org.opensearch.script.StringSortScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite string sort script leaf factory. */
@EqualsAndHashCode(callSuper = false)
public class CalciteStringSortScriptLeafFactory implements StringSortScript.LeafFactory {

  /** Generated code of calcite to execute. */
  private final Function1<DataContext, Object[]> function;

  /** Script parameters. */
  private final Map<String, Object> params;

  /** Search lookup. */
  private final SearchLookup lookup;

  /**
   * Stores the parameter names to the actual indices in SOURCES. Generate it in advance in factory
   * to save the process for each document*
   */
  private final Map<String, Integer> parametersToIndex;

  public CalciteStringSortScriptLeafFactory(
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
  public StringSortScript newInstance(LeafReaderContext context) throws IOException {
    return new CalciteStringSortScript(function, lookup, context, params, parametersToIndex);
  }
}
