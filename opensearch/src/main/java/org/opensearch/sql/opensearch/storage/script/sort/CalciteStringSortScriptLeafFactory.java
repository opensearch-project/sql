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

  public CalciteStringSortScriptLeafFactory(
      Function1<DataContext, Object[]> function, Map<String, Object> params, SearchLookup lookup) {
    this.function = function;
    this.params = params;
    this.lookup = lookup;
  }

  @Override
  public StringSortScript newInstance(LeafReaderContext context) throws IOException {
    return new CalciteStringSortScript(function, lookup, context, params);
  }
}
