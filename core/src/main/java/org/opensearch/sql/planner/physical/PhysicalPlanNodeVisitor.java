/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.write.TableWriteOperator;

/**
 * The visitor of {@link PhysicalPlan}.
 *
 * @param <R> return object type.
 * @param <C> context type.
 */
public abstract class PhysicalPlanNodeVisitor<R, C> {

  protected R visitNode(PhysicalPlan node, C context) {
    return null;
  }

  public R visitFilter(FilterOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitAggregation(AggregationOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitRename(RenameOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitTableScan(TableScanOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitTableWrite(TableWriteOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitProject(ProjectOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitWindow(WindowOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitRemove(RemoveOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitEval(EvalOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitNested(NestedOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitDedupe(DedupeOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitValues(ValuesOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitSort(SortOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitRareTopN(RareTopNOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitLimit(LimitOperator node, C context) {
    return visitNode(node, context);
  }

  public R visitMLCommons(PhysicalPlan node, C context) {
    return visitNode(node, context);
  }

  public R visitAD(PhysicalPlan node, C context) {
    return visitNode(node, context);
  }

  public R visitML(PhysicalPlan node, C context) {
    return visitNode(node, context);
  }

  public R visitCursorClose(CursorCloseOperator node, C context) {
    return visitNode(node, context);
  }
}
