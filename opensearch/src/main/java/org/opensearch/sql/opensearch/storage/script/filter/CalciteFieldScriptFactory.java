/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.opensearch.script.FieldScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite script factory that generates leaf factory. */
@EqualsAndHashCode
public class CalciteFieldScriptFactory implements FieldScript.Factory {

  /** Generated code of calcite to execute. */
  private final Function1<DataContext, Object[]> function;

  public CalciteFieldScriptFactory(Function1<DataContext, Object[]> function) {
    this.function = function;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public FieldScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new CalciteFieldScriptLeafFactory(function, params, lookup);
  }
}
