/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.expression.Expression;

/** Expression script leaf factory that produces script executor for each leaf. */
class ExpressionFilterScriptLeafFactory implements FilterScript.LeafFactory {

  /** Expression to execute. */
  private final Expression expression;

  /** Parameters for the expression. */
  private final Map<String, Object> params;

  /** Document lookup that returns doc values. */
  private final SearchLookup lookup;

  public ExpressionFilterScriptLeafFactory(
      Expression expression, Map<String, Object> params, SearchLookup lookup) {
    this.expression = expression;
    this.params = params;
    this.lookup = lookup;
  }

  @Override
  public FilterScript newInstance(LeafReaderContext ctx) {
    return new ExpressionFilterScript(expression, lookup, ctx, params);
  }
}
