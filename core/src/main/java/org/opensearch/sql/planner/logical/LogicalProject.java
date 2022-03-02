/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ParseExpression;

/**
 * Project field specified by the {@link LogicalProject#projectList}.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalProject extends LogicalPlan {

  @Getter
  private final List<NamedExpression> projectList;
  @Getter
  private final List<NamedExpression> parsedList;

  /**
   * Constructor of LogicalProject.
   */
  public LogicalProject(
      LogicalPlan child,
      List<NamedExpression> projectList,
      List<NamedExpression> parsedList) {
    super(Collections.singletonList(child));
    this.projectList = projectList;
    this.parsedList = parsedList;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }
}
