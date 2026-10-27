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

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class AppendPipe extends UnresolvedPlan {

  private UnresolvedPlan subQuery;

  private UnresolvedPlan child;

  public AppendPipe(UnresolvedPlan subQuery) {
    this.subQuery = subQuery;
  }

  @Override
  public AppendPipe attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAppendPipe(this, context);
  }
}
