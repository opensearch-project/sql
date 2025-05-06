/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Logical plan node of Project, the interface for building the list of searching fields. */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class Project extends UnresolvedPlan {
  @Setter private List<UnresolvedExpression> projectList;
  private final List<Argument> argExprList;
  private UnresolvedPlan child;

  public Project(List<UnresolvedExpression> projectList) {
    this.projectList = projectList;
    this.argExprList = Collections.emptyList();
  }

  public Project(List<UnresolvedExpression> projectList, List<Argument> argExprList) {
    this.projectList = projectList;
    this.argExprList = argExprList;
  }

  public boolean hasArgument() {
    return !argExprList.isEmpty();
  }

  /** The Project could been used to exclude fields from the source. */
  public boolean isExcluded() {
    if (hasArgument()) {
      Argument argument = argExprList.get(0);
      return (Boolean) argument.getValue().getValue();
    }
    return false;
  }

  @Override
  public Project attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {

    return nodeVisitor.visitProject(this, context);
  }
}
