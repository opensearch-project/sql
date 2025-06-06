/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Logical plan node of Filter, the interface for building filters in queries. */
@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
public class Filter extends UnresolvedPlan {
  private final UnresolvedExpression condition;
  private UnresolvedPlan child;

  public Filter(UnresolvedExpression condition) {
    this.condition = condition;
  }

  @Override
  public Filter attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitFilter(this, context);
  }
}
