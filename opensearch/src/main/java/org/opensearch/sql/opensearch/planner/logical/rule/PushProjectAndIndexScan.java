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
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Push Project list into ElasticsearchLogicalIndexScan.
 */
public class PushProjectAndIndexScan implements Rule<LogicalProject> {

  private final Capture<OpenSearchLogicalIndexScan> indexScanCapture;

  private final Pattern<LogicalProject> pattern;

  private Set<ReferenceExpression> pushDownProjects;

  /**
   * Constructor of MergeProjectAndIndexScan.
   */
  public PushProjectAndIndexScan() {
    this.indexScanCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalProject.class).matching(
        project -> {
          pushDownProjects = findReferenceExpressions(project.getProjectList());
          return !pushDownProjects.isEmpty();
        }).with(source()
        .matching(typeOf(OpenSearchLogicalIndexScan.class)
            .matching(indexScan -> !indexScan.hasProjects())
            .capturedAs(indexScanCapture)));

  }

  @Override
  public Pattern<LogicalProject> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalProject project,
                           Captures captures) {
    OpenSearchLogicalIndexScan indexScan = captures.get(indexScanCapture);
    indexScan.setProjectList(pushDownProjects);
    return new LogicalProject(indexScan, project.getProjectList());
  }
}
