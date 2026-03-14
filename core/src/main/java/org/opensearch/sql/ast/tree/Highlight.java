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
import org.opensearch.sql.ast.Node;

@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Highlight extends UnresolvedPlan {

  private UnresolvedPlan child;
  private final List<String> highlightArgs;

  @Override
  public Highlight attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
  }
}
