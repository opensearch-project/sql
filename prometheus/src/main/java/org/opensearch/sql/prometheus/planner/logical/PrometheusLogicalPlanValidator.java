/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.planner.logical;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.math3.util.Pair;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRemove;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalWindow;

public class PrometheusLogicalPlanValidator extends
    LogicalPlanNodeVisitor<Void, PrometheusLogicalPlanValidator.ValidatorContext> {

  @Override
  public Void visitRareTopN(LogicalRareTopN node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitDedupe(LogicalDedupe node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitProject(LogicalProject node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitWindow(LogicalWindow node, ValidatorContext context) {
    return null;
  }


  @Override
  public Void visitRemove(LogicalRemove node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitEval(LogicalEval node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitSort(LogicalSort node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitRename(LogicalRename node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitAggregation(LogicalAggregation node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitFilter(LogicalFilter node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitLimit(LogicalLimit node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitRelation(LogicalRelation node, ValidatorContext context) {
    return null;
  }

  @Override
  public Void visitNode(LogicalPlan plan, ValidatorContext context) {
    return null;
  }

  @Getter
  @AllArgsConstructor
  public static class ValidatorContext {

    private LogicalPlan parent;

    private List<String> errorMessages;

  }


}
