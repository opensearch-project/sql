/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;

/**
 * Alias abstraction that associate an unnamed expression with a name. The name information
 * preserved is useful for semantic analysis and response formatting eventually. This can avoid
 * restoring the info in toString() method which is inaccurate because original info is already
 * lost.
 */
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class Alias extends UnresolvedExpression {

  /**
   * The name to be associated with the result of computing delegated expression. In OpenSearch ppl,
   * the name is the actual alias of an expression
   */
  private final String name;

  /** Expression aliased. */
  private final UnresolvedExpression delegated;

  /** TODO. Optional field alias. This field is OpenSearch SQL-only */
  private final String alias;

  public Alias(String name, UnresolvedExpression expr) {
    this(name, expr, false);
  }

  public Alias(String name, UnresolvedExpression expr, String alias) {
    this(name, expr, alias, false);
  }

  public Alias(String name, UnresolvedExpression expr, boolean metaDataFieldAllowed) {
    this(name, expr, null, metaDataFieldAllowed);
  }

  /**
   * @param metadataFieldAllowed Whether do we allow metadata field as alias name. Should Only be
   *     true for SQL, see {@link Alias::newAliasAllowMetaMetaField}
   */
  private Alias(
      String name, UnresolvedExpression expr, String alias, boolean metadataFieldAllowed) {
    if (!metadataFieldAllowed && OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(name)) {
      throw new IllegalArgumentException(
          String.format("Cannot use metadata field [%s] as the alias.", name));
    }
    this.name = name;
    this.delegated = expr;
    this.alias = alias;
  }

  // TODO: Only for SQL. We never allow metadata field as alias but SQL view all select items as
  //  alias. Need to remove this tricky logic after SQL fix it.
  public static Alias newAliasAllowMetaMetaField(
      String name, UnresolvedExpression expr, String alias) {
    return new Alias(name, expr, alias, true);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAlias(this, context);
  }
}
