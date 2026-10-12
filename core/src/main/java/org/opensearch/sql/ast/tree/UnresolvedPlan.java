/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/** Abstract unresolved plan. */
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class UnresolvedPlan extends Node {
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }

  /**
   * Plans this node reads from that {@link #getChild()} does not expose, such as a sub-search or a
   * lookup table held in a separate field.
   *
   * <p>Override this when adding a command that holds a plan outside its child list. Consumers that
   * need every source a query reads from (for example index-name extraction for Query Insights)
   * walk {@code getChild()} plus this list, so a plan left out of both is silently invisible to
   * them. The returned plans must not duplicate {@code getChild()}.
   *
   * @return the additional source plans; empty by default
   */
  public List<UnresolvedPlan> getSources() {
    return ImmutableList.of();
  }

  public abstract UnresolvedPlan attach(UnresolvedPlan child);
}
