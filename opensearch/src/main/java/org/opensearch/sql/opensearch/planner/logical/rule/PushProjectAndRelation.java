/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.opensearch.planner.logical.rule.OptimizationRuleUtils.findReferenceExpressions;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.Set;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalIndexScan;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Push Project list into Relation. The transformed plan is Project - IndexScan
 */
public class PushProjectAndRelation implements Rule<LogicalProject> {

  private final Capture<LogicalRelation> relationCapture;

  private final Pattern<LogicalProject> pattern;

  private Set<ReferenceExpression> pushDownProjects;

  /**
   * Constructor of MergeProjectAndRelation.
   */
  public PushProjectAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalProject.class)
        .matching(project -> {
          pushDownProjects = findReferenceExpressions(project.getProjectList());
          return !pushDownProjects.isEmpty();
        })
        .with(source().matching(typeOf(LogicalRelation.class).capturedAs(relationCapture)));
  }

  @Override
  public Pattern<LogicalProject> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalProject project,
                           Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return new LogicalProject(
        OpenSearchLogicalIndexScan
            .builder()
            .relationName(relation.getRelationName())
            .projectList(findReferenceExpressions(project.getProjectList()))
            .build(),
        project.getProjectList(),
        project.getParseExpressionMap()
    );
  }
}
