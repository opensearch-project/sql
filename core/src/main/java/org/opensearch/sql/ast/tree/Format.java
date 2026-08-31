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

/** AST node that collapses input rows into a formatted search expression. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Format extends UnresolvedPlan {

  public static final String DEFAULT_MV_SEPARATOR = "OR";
  public static final int DEFAULT_MAX_RESULTS = 0;
  public static final String DEFAULT_ROW_PREFIX = "(";
  public static final String DEFAULT_COLUMN_PREFIX = "(";
  public static final String DEFAULT_COLUMN_SEPARATOR = "AND";
  public static final String DEFAULT_COLUMN_END = ")";
  public static final String DEFAULT_ROW_SEPARATOR = "OR";
  public static final String DEFAULT_ROW_END = ")";
  public static final String DEFAULT_EMPTY_STRING = "NOT ()";

  private final String mvSeparator;
  private final int maxResults;
  private final String rowPrefix;
  private final String columnPrefix;
  private final String columnSeparator;
  private final String columnEnd;
  private final String rowSeparator;
  private final String rowEnd;
  private final String emptyString;

  /** Whether the result is consumed by an enclosing search rather than returned to the user. */
  private boolean implicit;

  private UnresolvedPlan child;

  @Override
  public Format attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return child == null ? ImmutableList.of() : ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
    return visitor.visitFormat(this, context);
  }
}
