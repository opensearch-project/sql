/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.sort;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.script.StringSortScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite string sort script factory that generates leaf factory. */
@EqualsAndHashCode(callSuper = false)
public class CalciteStringSortScriptFactory implements StringSortScript.Factory {

  /** Generated code of calcite to execute. */
  private final Function1<DataContext, Object[]> function;

  public CalciteStringSortScriptFactory(
      Function1<DataContext, Object[]> function, RelDataType type) {
    this.function = function;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public StringSortScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new CalciteStringSortScriptLeafFactory(function, params, lookup);
  }
}
