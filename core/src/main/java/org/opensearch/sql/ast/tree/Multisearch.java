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
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * Logical plan node of Multisearch, which combines results from multiple search queries. Similar to
 * UNION ALL operation, it executes multiple subsearches and combines their results.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Multisearch extends UnresolvedPlan {

  /** List of subsearch plans to execute and combine. */
  private final List<UnresolvedPlan> subsearches;

  /** The main query/child that multisearch attaches to (if any). */
  private UnresolvedPlan child;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    // If there's a child (main query), return it along with subsearches
    // Otherwise just return subsearches
    if (this.child == null) {
      return subsearches;
    } else {
      return ImmutableList.<Node>builder().add(this.child).addAll(subsearches).build();
    }
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMultisearch(this, context);
  }
}
