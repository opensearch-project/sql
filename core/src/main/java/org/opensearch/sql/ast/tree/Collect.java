/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * AST node for the {@code collect} command: a terminal write sink appending pipeline rows to a
 * pre-existing destination index. See the collect RFC for design.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class Collect extends UnresolvedPlan {

  private UnresolvedPlan child;

  private final String indexName;

  private String source;

  private String host;

  private String sourcetype;

  private String marker;

  /** Dry-run: stamp and pass rows through, but do not write to the destination. */
  private boolean testmode = false;

  @Override
  public Collect attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitCollect(this, context);
  }
}
