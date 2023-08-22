/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.expression.Expression;

/** Expression script leaf factory that produces script executor for each leaf. */
public class ExpressionAggregationScriptLeafFactory implements AggregationScript.LeafFactory {

  /** Expression to execute. */
  private final Expression expression;

  /** Expression to execute. */
  private final Map<String, Object> params;

  /** Expression to execute. */
  private final SearchLookup lookup;

  /** Constructor of ExpressionAggregationScriptLeafFactory. */
  public ExpressionAggregationScriptLeafFactory(
      Expression expression, Map<String, Object> params, SearchLookup lookup) {
    this.expression = expression;
    this.params = params;
    this.lookup = lookup;
  }

  @Override
  public AggregationScript newInstance(LeafReaderContext ctx) {
    return new ExpressionAggregationScript(expression, lookup, ctx, params);
  }

  @Override
  public boolean needs_score() {
    return false;
  }
}
