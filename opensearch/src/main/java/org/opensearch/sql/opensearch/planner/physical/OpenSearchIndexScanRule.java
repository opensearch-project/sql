/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.plan.RelOptTable;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

public interface OpenSearchIndexScanRule {

  // CalciteOpenSearchIndexScan doesn't allow push-down anymore (except Sort under some strict
  // condition) after Aggregate push-down.
  static boolean test(CalciteLogicalIndexScan scan) {
    if (scan.getPushDownContext().isAggregatePushed()) return false;
    final RelOptTable table = scan.getTable();
    return table.unwrap(OpenSearchIndex.class) != null;
  }

  static boolean isLimitPushed(CalciteLogicalIndexScan scan) {
    return scan.getPushDownContext().isLimitPushed();
  }
}
