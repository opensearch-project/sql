/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.lookup.SearchLookup;

/** Calcite script leaf factory that produces script executor for each leaf. */
class CalciteAggregationScriptLeafFactory implements AggregationScript.LeafFactory {

  private final Function1<DataContext, Object[]> function;
  private final RelDataType type;

  /** Parameters for the calcite script. */
  private final Map<String, Object> params;

  /** Document lookup that returns doc values. */
  private final SearchLookup lookup;

  public CalciteAggregationScriptLeafFactory(
      Function1<DataContext, Object[]> function,
      RelDataType type,
      Map<String, Object> params,
      SearchLookup lookup) {
    this.function = function;
    this.type = type;
    this.params = params;
    this.lookup = lookup;
  }

  @Override
  public AggregationScript newInstance(LeafReaderContext ctx) {
    return new CalciteAggregationScript(function, type, lookup, ctx, params);
  }

  @Override
  public boolean needs_score() {
    return false;
  }
}
