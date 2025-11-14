/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.sort;

import java.io.IOException;
import java.util.Map;
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

  public CalciteNumberSortScriptLeafFactory(
      Function1<DataContext, Object[]> function, Map<String, Object> params, SearchLookup lookup) {
    this.function = function;
    this.params = params;
    this.lookup = lookup;
  }

  @Override
  public NumberSortScript newInstance(LeafReaderContext context) throws IOException {
    return new CalciteNumberSortScript(function, lookup, context, params);
  }

  @Override
  public boolean needs_score() {
    return false;
  }
}
