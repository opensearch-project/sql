/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite script factory that generates leaf factory. */
@EqualsAndHashCode
public class CalciteFilterScriptFactory implements FilterScript.Factory {

  /** Generated code of calcite to execute. */
  private final Function1<DataContext, Object[]> function;

  public CalciteFilterScriptFactory(Function1<DataContext, Object[]> function) {
    this.function = function;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public FilterScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new CalciteFilterScriptLeafFactory(function, params, lookup);
  }
}
