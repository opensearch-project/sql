/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.executor.QueryType;

/** Query Statement. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Query extends Statement {

  protected final UnresolvedPlan plan;
  protected final int fetchSize;
  private final QueryType queryType;

  /**
   * Whether the request asked for metadata fields such as {@code _id}, {@code _index} and {@code
   * _score} to be kept in the result.
   */
  private final boolean includeMetadata;

  private HighlightConfig highlightConfig;

  public Query(UnresolvedPlan plan, int fetchSize, QueryType queryType) {
    this(plan, fetchSize, queryType, false);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
