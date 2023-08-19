/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.analysis;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.opensearch.ast.expression.HighlightFunction;
import org.opensearch.sql.opensearch.ast.expression.NestedAllTupleFields;
import org.opensearch.sql.opensearch.ast.expression.OpenSearchFunction;
import org.opensearch.sql.opensearch.ast.expression.RelevanceFieldList;
import org.opensearch.sql.opensearch.ast.expression.ScoreFunction;

public interface OpenSearchAbstractNodeVisitor<T, C> extends AbstractNodeVisitor<T, C> {
  default T visit(Node node, C context) {
    return null;
  }

  /**
   * Visit child node.
   *
   * @param node {@link Node}
   * @param context Context
   * @return Return Type.
   */
  default T visitChildren(Node node, C context) {
    T result = defaultResult();

    for (Node child : node.getChild()) {
      T childResult = child.accept(this, context);
      result = aggregateResult(result, childResult);
    }
    return result;
  }

  private T defaultResult() {
    return null;
  }

  private T aggregateResult(T aggregate, T nextResult) {
    return nextResult;
  }

  default T visitNestedAllTupleFields(NestedAllTupleFields node, C context) {
    return visitChildren(node, context);
  }

  default T visitHighlightFunction(HighlightFunction node, C context) {
    return visitChildren(node, context);
  }

  default T visitRelevanceFieldList(RelevanceFieldList node, C context) {
    return visitChildren(node, context);
  }

  default T visitScoreFunction(ScoreFunction node, C context) {
    return visitChildren(node, context);
  }

  default T visitOpenSearchFunction(OpenSearchFunction node, C context) {
    return visitChildren(node, context);
  }
}
