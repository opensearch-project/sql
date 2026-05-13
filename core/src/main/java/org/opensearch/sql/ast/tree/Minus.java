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

/** Logical plan node for Minus (EXCEPT) operation. Returns rows in left that are not in right. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Minus extends UnresolvedPlan {
  private final List<UnresolvedPlan> children;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    List<UnresolvedPlan> newChildren =
        ImmutableList.<UnresolvedPlan>builder().add(child).addAll(children).build();
    return new Minus(newChildren);
  }

  @Override
  public List<? extends UnresolvedPlan> getChild() {
    return children;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMinus(this, context);
  }
}
