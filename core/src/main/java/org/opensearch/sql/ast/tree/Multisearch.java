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

/** Logical plan node for Multisearch operation. Combines results from multiple search queries. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Multisearch extends UnresolvedPlan {

  private UnresolvedPlan child;
  private final List<UnresolvedPlan> subsearches;

  public Multisearch(List<UnresolvedPlan> subsearches) {
    this.subsearches = subsearches;
  }

  @Override
  public Multisearch attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    if (this.child == null) {
      return ImmutableList.copyOf(subsearches);
    } else {
      return ImmutableList.<UnresolvedPlan>builder().add(this.child).addAll(subsearches).build();
    }
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMultisearch(this, context);
  }
}
