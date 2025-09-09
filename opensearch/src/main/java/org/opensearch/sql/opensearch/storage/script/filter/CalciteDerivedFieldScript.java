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
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/**
 * Calcite script executor that executes the generated code on each document and determine if the
 * document is supposed to be project script out or not.
 */
@EqualsAndHashCode(callSuper = false)
class CalciteDerivedFieldScript extends DerivedFieldScript {

  /** Calcite Script. */
  private final CalciteScript calciteScript;

  private final SourceLookup sourceLookup;

  public CalciteDerivedFieldScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
  }

  @Override
  public void execute() {
    Object result = calciteScript.execute(this.getDoc(), this.sourceLookup)[0];
    addEmittedValue(result);
  }
}
