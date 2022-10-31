/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.implementor;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.storage.PrometheusMetricScan;

/**
 * Default Implementor of Logical plan for prometheus.
 */
@RequiredArgsConstructor
public class PrometheusDefaultImplementor
    extends DefaultImplementor<PrometheusMetricScan> {

  @Override
  public PhysicalPlan visitRelation(LogicalRelation node,
                                    PrometheusMetricScan context) {
    return context;
  }

  // Since getFieldTypes include labels
  // we are explicitly specifying the output column names;
  @Override
  public PhysicalPlan visitProject(LogicalProject node, PrometheusMetricScan context) {
    List<NamedExpression> finalProjectList = new ArrayList<>();
    finalProjectList.add(
        new NamedExpression(LABELS, new ReferenceExpression(LABELS, ExprCoreType.STRING)));
    finalProjectList.add(
        new NamedExpression(TIMESTAMP,
            new ReferenceExpression(TIMESTAMP, ExprCoreType.TIMESTAMP)));
    finalProjectList.add(
        new NamedExpression(VALUE, new ReferenceExpression(VALUE, ExprCoreType.DOUBLE)));
    return new ProjectOperator(visitChild(node, context), finalProjectList,
        node.getNamedParseExpressions());
  }

}