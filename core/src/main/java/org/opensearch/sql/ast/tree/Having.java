/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Represents unresolved HAVING clause, its child can be Aggregation. Having without aggregation
 * equals to {@link Filter}
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Having extends UnresolvedPlan {
  private List<UnresolvedExpression> aggregators;
  private UnresolvedExpression condition;
  private UnresolvedPlan child;

  public Having(List<UnresolvedExpression> aggregators, UnresolvedExpression condition) {
    this.aggregators = aggregators;
    this.condition = condition;
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitHaving(this, context);
  }
}
