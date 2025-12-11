/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.executor.QueryType;

/** Query Statement. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Query extends Statement {

  protected final UnresolvedPlan plan;
  protected final int fetchSize;
  private final QueryType queryType;

  /** Pagination offset (0-based). Only used when fetchSize > 0. */
  protected final int paginationOffset;

  /**
   * Constructor for backward compatibility (no pagination offset).
   *
   * @param plan the unresolved plan
   * @param fetchSize page size (0 = no pagination)
   * @param queryType the query type
   */
  public Query(UnresolvedPlan plan, int fetchSize, QueryType queryType) {
    this(plan, fetchSize, queryType, 0);
  }

  /**
   * Constructor with pagination support.
   *
   * @param plan the unresolved plan
   * @param fetchSize page size (0 = no pagination)
   * @param queryType the query type
   * @param paginationOffset offset for pagination (0-based)
   */
  public Query(UnresolvedPlan plan, int fetchSize, QueryType queryType, int paginationOffset) {
    this.plan = plan;
    this.fetchSize = fetchSize;
    this.queryType = queryType;
    this.paginationOffset = paginationOffset;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
