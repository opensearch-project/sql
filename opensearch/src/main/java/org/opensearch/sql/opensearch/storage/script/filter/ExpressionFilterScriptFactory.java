/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.expression.Expression;

/** Expression script factory that generates leaf factory. */
@EqualsAndHashCode
public class ExpressionFilterScriptFactory implements FilterScript.Factory {

  /** Expression to execute. */
  private final Expression expression;

  public ExpressionFilterScriptFactory(Expression expression) {
    this.expression = expression;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public FilterScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new ExpressionFilterScriptLeafFactory(expression, params, lookup);
  }
}
