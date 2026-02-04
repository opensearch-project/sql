/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;

/**
 * AST node for graphLookup command. Performs BFS graph traversal on a lookup table.
 *
 * <p>Example: source=employees | graphLookup employees fromField=manager toField=name maxDepth=3
 * depthField=level direction=uni as hierarchy
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class GraphLookup extends UnresolvedPlan {
  /** Direction mode for graph traversal. */
  public enum Direction {
    /** Unidirectional - traverse edges in one direction only. */
    UNI,
    /** Bidirectional - traverse edges in both directions. */
    BI
  }

  /** Target table for graph traversal lookup. */
  private final UnresolvedPlan fromTable;

  /** Field in sourceTable to start with. */
  private final Field startField;

  /** Field in fromTable that represents the outgoing edge. */
  private final Field fromField;

  /** Field in input/fromTable to match against for traversal. */
  private final Field toField;

  /** Output field name for collected traversal results. */
  private final Field as;

  /** Maximum traversal depth. Zero means no limit. */
  private final Literal maxDepth;

  /** Optional field name to include recursion depth in output. */
  private @Nullable final Field depthField;

  /** Direction mode: UNI (default) or BIO for bidirectional. */
  private final Direction direction;

  private UnresolvedPlan child;

  public String getDepthFieldName() {
    return depthField == null ? null : depthField.toString();
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return child == null ? ImmutableList.of() : ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
    return visitor.visitGraphLookup(this, context);
  }
}
