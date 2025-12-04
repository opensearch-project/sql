/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

/** Base class for search expressions that get converted to query_string syntax. */
public abstract class SearchExpression extends UnresolvedExpression {

  /**
   * Convert this search expression to query_string syntax.
   *
   * @return the query string representation
   */
  public abstract String toQueryString();

  /**
   * Convert the search expression to anonymized string
   *
   * @return the anonymized string
   */
  public abstract String toAnonymizedString();

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }
}
