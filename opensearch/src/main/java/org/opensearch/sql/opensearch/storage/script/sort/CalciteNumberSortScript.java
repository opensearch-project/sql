/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.sort;

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

  public CalciteNumberSortScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    // TODO: we'd better get source from the leafLookup of super once it's available
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
  }

  @Override
  public double execute() {
    Object[] result = calciteScript.execute(this.getDoc(), this.sourceLookup);
    if (result == null || result.length == 0 || result[0] == null) {
      return 0.0;
    }

    Object value = result[0];
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    // Try to parse as double if it's a string
    if (value instanceof String) {
      try {
        return Double.parseDouble((String) value);
      } catch (NumberFormatException e) {
        return 0.0;
      }
    }

    return 0.0;
  }
}
