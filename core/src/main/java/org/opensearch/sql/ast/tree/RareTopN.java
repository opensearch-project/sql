/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node represent RareTopN operation.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class RareTopN extends UnresolvedPlan {

  private UnresolvedPlan child;
  private final CommandType commandType;
  private final List<Argument> noOfResults;
  private final List<Field> fields;
  private final List<UnresolvedExpression> groupExprList;

  @Override
  public RareTopN attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return Collections.singletonList(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRareTopN(this, context);
  }

  public enum CommandType {
    TOP,
    RARE
  }
}
