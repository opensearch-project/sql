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
import org.opensearch.script.StringSortScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/** Calcite string sort script. */
@EqualsAndHashCode(callSuper = false)
public class CalciteStringSortScript extends StringSortScript {

  /** Calcite script. */
  private final CalciteScript calciteScript;

  private final SourceLookup sourceLookup;
  private final LeafReaderContext context;
  private final Map<String, Integer> parametersToIndex;

  private final boolean missingMax;

  private static final String MAX_SENTINEL = "\uFFFF\uFFFF_NULL_PLACEHOLDER_";
  private static final String MIN_SENTINEL = "\u0000\u0000_NULL_PLACEHOLDER_";

  public CalciteStringSortScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params,
      Map<String, Integer> parametersToIndex) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    // TODO: we'd better get source from the leafLookup of super once it's available
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
    this.context = context;
    this.parametersToIndex = parametersToIndex;
    this.missingMax = (Boolean) params.getOrDefault(MISSING_MAX, false);
  }

  @Override
  public void setDocument(int docid) {
    super.setDocument(docid);
    this.sourceLookup.setSegmentAndDocument(context, docid);
  }

  @Override
  public String execute() {
    Object value = calciteScript.execute(this.getDoc(), this.sourceLookup, parametersToIndex)[0];
    // There is a limitation here when the String value is larger or smaller than sentinel values.
    // It can't guarantee the lexigraphic ordering between null and special strings.
    if (value == null) {
      return this.missingMax ? MAX_SENTINEL : MIN_SENTINEL;
    }
    return value.toString();
  }
}
