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
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node represent Timechart operation.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class Timechart extends UnresolvedPlan {
  private UnresolvedPlan child;
  private UnresolvedExpression spanExpression;
  private UnresolvedExpression aggregateFunction;
  private UnresolvedExpression byField;

  @Override
  public Timechart attach(UnresolvedPlan child) {
    return new Timechart(child, spanExpression, aggregateFunction, byField);
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitTimechart(this, context);
  }
}