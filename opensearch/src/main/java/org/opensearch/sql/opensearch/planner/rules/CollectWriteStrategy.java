/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;

/**
 * Detects whether a {@code collect} input is a full-table-scope copy (a scan, optionally under
 * row-preserving filters), which is the only shape expressible as a reindex {@code source} query.
 * Groundwork for a future reindex fast lane; conservative by design (a false negative just falls
 * back to the always-correct batched-bulk path).
 */
public final class CollectWriteStrategy {

  private CollectWriteStrategy() {}

  public static boolean isFullTableScope(RelNode input) {
    RelNode node = unwrap(input);
    while (true) {
      if (node instanceof TableScan) {
        return true;
      }
      if (node instanceof Filter && node.getInputs().size() == 1) {
        node = unwrap(node.getInputs().get(0));
        continue;
      }
      // Anything else (Project, Aggregate, Join, Sort, ...) changes the rows: not a plain copy.
      return false;
    }
  }

  /** Peels a Volcano {@link RelSubset} to its best (or original) concrete rel. */
  private static RelNode unwrap(RelNode node) {
    if (node instanceof RelSubset subset) {
      RelNode best = subset.getBest();
      return best != null ? best : subset.getOriginal();
    }
    return node;
  }
}
