/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.field;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.FieldScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/** Calcite script executor that produce dynamic values. */
@EqualsAndHashCode(callSuper = false)
public class CalciteFieldScript extends FieldScript {

  /** Calcite Script. */
  private final CalciteScript calciteScript;

  private final SourceLookup sourceLookup;

  private final Map<String, Integer> parametersToIndex;

  /**
   * Creates a new CalciteFieldScript.
   *
   * @param function the Calcite function to execute
   * @param lookup the search lookup for document access
   * @param context the leaf reader context
   * @param params the script parameters
   * @param parametersToIndex mapping from parameter names to indices
   */
  public CalciteFieldScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params,
      Map<String, Integer> parametersToIndex) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
    this.parametersToIndex = parametersToIndex;
  }

  @Override
  public Object execute() {
    Object[] results =
        calciteScript.execute(this.getDoc(), this.sourceLookup, this.parametersToIndex);
    if (results == null || results.length == 0) {
      return null;
    }
    return results[0];
  }
}
