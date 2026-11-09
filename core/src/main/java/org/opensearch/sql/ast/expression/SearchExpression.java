/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.function.Function;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.data.type.ExprType;

/** Base class for search expressions that get converted to query_string syntax. */
public abstract class SearchExpression extends UnresolvedExpression {

  /**
   * Convert this search expression to query_string syntax without field-type awareness.
   *
   * @return the query string representation
   */
  public String toQueryString() {
    return toQueryString(f -> null);
  }

  /**
   * Convert this search expression to query_string syntax, using {@code fieldTypeResolver} to
   * resolve the OpenSearch type of a field when the emission depends on whether the field is
   * keyword vs. text. When the resolver returns {@code null}, emission falls back to the
   * field-type-agnostic form (same as {@link #toQueryString()}).
   *
   * @param fieldTypeResolver maps a field name to its resolved {@link ExprType}, or null when
   *     unknown
   * @return the query string representation
   */
  public abstract String toQueryString(Function<String, ExprType> fieldTypeResolver);

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
