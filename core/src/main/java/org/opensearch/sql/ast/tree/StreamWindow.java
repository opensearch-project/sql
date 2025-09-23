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

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class StreamWindow extends UnresolvedPlan {

  private final List<UnresolvedExpression> windowFunctionList;
  private final List<UnresolvedExpression> groupList;
  private final boolean current;
  private final int window;
  private final boolean global;
  private final UnresolvedExpression resetBefore;
  private final UnresolvedExpression resetAfter;
  @ToString.Exclude private UnresolvedPlan child;

  /** StreamWindow Constructor. */
  public StreamWindow(
      List<UnresolvedExpression> windowFunctionList,
      List<UnresolvedExpression> groupList,
      boolean current,
      int window,
      boolean global,
      UnresolvedExpression resetBefore,
      UnresolvedExpression resetAfter) {
    this.windowFunctionList = windowFunctionList;
    this.groupList = groupList;
    this.current = current;
    this.window = window;
    this.global = global;
    this.resetBefore = resetBefore;
    this.resetAfter = resetAfter;
  }

  public boolean isCurrent() {
    return current;
  }

  public boolean isGlobal() {
    return global;
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
