/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.sort;

import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.MISSING_MAX;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.NumberSortScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/** Calcite number sort script. */
@EqualsAndHashCode(callSuper = false)
public class CalciteNumberSortScript extends NumberSortScript {

  /** Calcite script. */
  private final CalciteScript calciteScript;

  private final SourceLookup sourceLookup;
  private final boolean missingMax;
  private final Map<String, Integer> parametersToIndex;

  public CalciteNumberSortScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params,
      Map<String, Integer> parametersToIndex) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    // TODO: we'd better get source from the leafLookup of super once it's available
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
    this.parametersToIndex = parametersToIndex;
    this.missingMax = (Boolean) params.getOrDefault(MISSING_MAX, false);
  }

  @Override
  public double execute() {
    Object value =
        calciteScript.execute(this.getDoc(), this.sourceLookup, this.parametersToIndex)[0];
    // There is a limitation here when the Double value is exactly theoretical min/max value.
    // It can't distinguish the ordering between null and exact Double.NEGATIVE_INFINITY or
    // Double.NaN.
    if (value == null) {
      return this.missingMax ? Double.NaN : Double.NEGATIVE_INFINITY;
    }
    return ((Number) value).doubleValue();
  }
}
