/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.AggSpec;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;

/**
 * Inspects a Calcite plan tree to determine whether execution will be expensive. Detects: (1)
 * user-defined functions (REX_EXTRACT, PARSE, etc.) that become per-document scripts, (2) join
 * nodes requiring in-memory merge, and (3) window functions requiring in-memory evaluation.
 *
 * <p>Works on both logical plans (before optimization, where PushDownContext is empty) and physical
 * plans (after optimization, where PushDownContext tracks scripts).
 */
public final class ScriptDetector {

  private ScriptDetector() {}

  /**
   * Returns true if the plan contains patterns indicating expensive execution: UDF calls that
   * produce scripts, join nodes, or window functions.
   */
  public static boolean hasScripts(RelNode plan) {
    boolean[] found = {false};
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (found[0]) {
          return;
        }
        // Physical plan: check PushDownContext for scripts already detected by optimizer
        if (node instanceof AbstractCalciteIndexScan scan) {
          found[0] = scanHasScripts(scan);
        }
        // Logical plan: detect join nodes (always require in-memory processing)
        if (!found[0] && node instanceof Join) {
          found[0] = true;
        }
        // Logical plan: detect window rel nodes (eventstats, dedup patterns)
        if (!found[0] && node instanceof Window) {
          found[0] = true;
        }
        // Logical plan: check projections for UDFs or window expressions
        if (!found[0] && node instanceof Project project) {
          found[0] = projectHasExpensiveExpressions(project);
        }
        if (!found[0]) {
          super.visit(node, ordinal, parent);
        }
      }
    }.go(plan);
    return found[0];
  }

  private static boolean scanHasScripts(AbstractCalciteIndexScan scan) {
    PushDownContext ctx = scan.getPushDownContext();
    if (ctx.isScriptPushed()) {
      return true;
    }
    if (ctx.isSortExprPushed()) {
      return true;
    }
    AggSpec aggSpec = ctx.getAggSpec();
    return aggSpec != null && aggSpec.getScriptCount() > 0;
  }

  private static boolean projectHasExpensiveExpressions(Project project) {
    for (RexNode expr : project.getProjects()) {
      if (hasExpensiveRex(expr)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasExpensiveRex(RexNode expr) {
    boolean[] found = {false};
    expr.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (call.getOperator() instanceof SqlUserDefinedFunction) {
              found[0] = true;
              return null;
            }
            if (call instanceof RexOver) {
              found[0] = true;
              return null;
            }
            return super.visitCall(call);
          }
        });
    return found[0];
  }
}
