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
 * AST node for the {@code outputlookup} command: a terminal write sink that materializes pipeline
 * rows into a lookup index. Overwrite-by-default (set {@code append=true} to append instead). See
 * the outputlookup PPL design.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class OutputLookup extends UnresolvedPlan {

  private UnresolvedPlan child;

  /** Destination lookup name. */
  private final String indexName;

  /** false (default) overwrites the destination; true appends to it. */
  private boolean append = false;

  /** true (default) clears the destination on empty results; false keeps it. */
  private boolean overrideIfEmpty = true;

  /**
   * Fields whose values form the document {@code _id} for upsert; empty means auto-generated id.
   */
  private List<String> keyFields = java.util.List.of();

  /** Cap on the number of rows written; null means unbounded. */
  private Integer max;

  @Override
  public OutputLookup attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitOutputLookup(this, context);
  }
}
