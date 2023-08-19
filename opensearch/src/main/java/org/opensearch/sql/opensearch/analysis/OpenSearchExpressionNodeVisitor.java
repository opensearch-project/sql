/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.analysis;

import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.opensearch.ast.expression.HighlightExpression;
import org.opensearch.sql.opensearch.ast.expression.OpenSearchFunctionExpression;

public abstract class OpenSearchExpressionNodeVisitor<T, C> extends ExpressionNodeVisitor<T, C> {

  public T visitHighlight(HighlightExpression node, C context) {
    return visitNode(node, context);
  }

  public T visitOpenSearchFunction(OpenSearchFunctionExpression node, C context) {
    return visitNode(node, context);
  }
}
