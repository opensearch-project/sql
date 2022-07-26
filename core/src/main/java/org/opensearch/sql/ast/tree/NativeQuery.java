/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Logical plan node of Native Query Relation, the interface for building the searching sources.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class NativeQuery extends UnresolvedPlan {

  private final UnresolvedExpression catalog;

  @Getter
  private List<Argument> argExprList;

  /**
   * Get Catalog Name.
   * @return catalog name
   */
  public String getCatalogName() {
    return catalog != null ? catalog.toString() : null;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitNativeQuery(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return this;
  }

}
