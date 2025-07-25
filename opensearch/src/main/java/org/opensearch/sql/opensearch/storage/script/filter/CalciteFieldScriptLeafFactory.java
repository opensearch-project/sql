/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.FieldScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite script leaf factory that produces script executor for each leaf. */
class CalciteFieldScriptLeafFactory implements FieldScript.LeafFactory {

  private final Function1<DataContext, Object[]> function;

  /** Parameters for the calcite script. */
  private final Map<String, Object> params;

  /** Document lookup that returns doc values. */
  private final SearchLookup lookup;

  public CalciteFieldScriptLeafFactory(
      Function1<DataContext, Object[]> function, Map<String, Object> params, SearchLookup lookup) {
    this.function = function;
    this.params = params;
    this.lookup = lookup;
  }

  @Override
  public FieldScript newInstance(LeafReaderContext ctx) {
    return new CalciteFieldScript(function, lookup, ctx, params);
  }
}
