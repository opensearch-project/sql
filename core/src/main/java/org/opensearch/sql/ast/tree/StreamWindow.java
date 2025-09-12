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
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class StreamWindow extends UnresolvedPlan {

  private final List<UnresolvedExpression> windowFunctionList;
  private final List<Argument> argExprList;
  @ToString.Exclude private UnresolvedPlan child;

  /** StreamWindow Constructor without optional argument. */
  public StreamWindow(List<UnresolvedExpression> windowFunctionList) {
    this(windowFunctionList, Collections.emptyList());
  }

  /** StreamWindow Constructor. */
  public StreamWindow(List<UnresolvedExpression> windowFunctionList, List<Argument> argExprList) {
    this.windowFunctionList = windowFunctionList;
    this.argExprList = argExprList;
  }

  public boolean hasArgument() {
    return !argExprList.isEmpty();
  }

  @Override
  public StreamWindow attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitStreamWindow(this, context);
  }
}
