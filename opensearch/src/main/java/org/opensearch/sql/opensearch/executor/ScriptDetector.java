/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Inspects a Calcite plan tree to determine whether any scan node requires script evaluation. A
 * plan with scripts cannot be fully pushed down to OpenSearch and will perform in-memory
 * evaluation, making it a candidate for the slow worker pool.
 */
public final class ScriptDetector {

  private ScriptDetector() {}

  /** Returns true if any scan node in the plan has scripts pushed. */
  public static boolean hasScripts(RelNode plan) {
    boolean[] found = {false};
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (!found[0] && node instanceof AbstractCalciteIndexScan scan) {
          found[0] = scan.isScriptPushed();
        }
        if (!found[0]) {
          super.visit(node, ordinal, parent);
        }
      }
    }.go(plan);
    return found[0];
  }
}
