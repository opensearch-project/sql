/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
public class Join extends UnresolvedPlan {
  private final UnresolvedPlan left;
  private final UnresolvedPlan right;
  private final JoinType joinType;
  private final UnresolvedExpression joinCondition;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(left, right);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitJoin(this, context);
  }

  public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    SEMI,
    ANTI,
    CROSS,
    FULL
  }
}
