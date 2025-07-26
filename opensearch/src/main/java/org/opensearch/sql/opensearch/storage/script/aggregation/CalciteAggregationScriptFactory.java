/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite script factory that generates leaf factory. */
@EqualsAndHashCode
public class CalciteAggregationScriptFactory implements AggregationScript.Factory {

  /** Generated code of calcite to execute. */
  private final Function1<DataContext, Object[]> function;

  private final RelDataType type;

  public CalciteAggregationScriptFactory(
      Function1<DataContext, Object[]> function, RelDataType type) {
    this.function = function;
    this.type = type;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public AggregationScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new CalciteAggregationScriptLeafFactory(function, type, params, lookup);
  }
}
