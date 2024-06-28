/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Arrays;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.ReferenceExpression;

/** Logical Lookup Plan. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalLookup extends LogicalPlan {

  private final String indexName;
  private final Map<ReferenceExpression, ReferenceExpression> matchFieldMap;
  private final Map<ReferenceExpression, ReferenceExpression> copyFieldMap;
  private final Boolean appendOnly;

  /** Constructor of LogicalLookup. */
  public LogicalLookup(
      LogicalPlan child,
      String indexName,
      Map<ReferenceExpression, ReferenceExpression> matchFieldMap,
      Boolean appendOnly,
      Map<ReferenceExpression, ReferenceExpression> copyFieldMap) {
    super(Arrays.asList(child));
    this.indexName = indexName;
    this.copyFieldMap = copyFieldMap;
    this.matchFieldMap = matchFieldMap;
    this.appendOnly = appendOnly;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLookup(this, context);
  }
}
