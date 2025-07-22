/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/**
 * Calcite script executor that executes the generated code on each document and determine if the
 * document is supposed to be filtered out or not.
 */
@EqualsAndHashCode(callSuper = false)
class CalciteFilterScript extends FilterScript {

  /** Calcite Script. */
  private final CalciteScript calciteScript;

  public CalciteFilterScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
  }

  @Override
  public boolean execute() {
    Object result = calciteScript.execute(this::getDoc)[0];
    // The result should be type of BOOLEAN_NULLABLE. Treat it as false if null
    return result != null && (boolean) result;
  }
}
