/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Logical plan node of Aggregation, the interface for building aggregation actions in queries.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Aggregation extends UnresolvedPlan {
  private List<UnresolvedExpression> aggExprList;
  private List<UnresolvedExpression> sortExprList;
  private List<UnresolvedExpression> groupExprList;
  private UnresolvedExpression span;
  private List<Argument> argExprList;
  private UnresolvedPlan child;

  /**
   * Aggregation Constructor without span and argument.
   */
  public Aggregation(List<UnresolvedExpression> aggExprList,
                     List<UnresolvedExpression> sortExprList,
                     List<UnresolvedExpression> groupExprList) {
    this(aggExprList, sortExprList, groupExprList, null, Collections.emptyList());
  }

  /**
   * Aggregation Constructor.
   */
  public Aggregation(List<UnresolvedExpression> aggExprList,
                     List<UnresolvedExpression> sortExprList,
                     List<UnresolvedExpression> groupExprList,
                     UnresolvedExpression span,
                     List<Argument> argExprList) {
    this.aggExprList = aggExprList;
    this.sortExprList = sortExprList;
    this.groupExprList = groupExprList;
    this.span = span;
    this.argExprList = argExprList;
  }

  public boolean hasArgument() {
    return !aggExprList.isEmpty();
  }

  @Override
  public Aggregation attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAggregation(this, context);
  }
}
