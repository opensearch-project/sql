/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AttributeList;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class FieldSummary extends UnresolvedPlan {
  private UnresolvedPlan child;
  private List<UnresolvedExpression> includeFields;

  public FieldSummary(List<UnresolvedExpression> collect) {
    collect.forEach(
        exp -> {
          if (exp instanceof AttributeList) {
            this.includeFields = ((AttributeList) exp).getAttrList();
          }
        });
  }

  @Override
  public List<? extends Node> getChild() {
    return child == null ? List.of() : List.of(child);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitFieldSummary(this, context);
  }

  @Override
  public FieldSummary attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }
}
