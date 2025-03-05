package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.plan.RelOptTable;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalOSIndexScan;

public interface OpenSearchIndexScanRule {

  // CalciteOpenSearchIndexScan doesn't allow push-down anymore (except Sort under some strict
  // condition) after Aggregate push-down.
  static boolean test(CalciteLogicalOSIndexScan scan) {
    if (scan.getPushDownContext().isAggregatePushed()) return false;
    final RelOptTable table = scan.getTable();
    return table.unwrap(OpenSearchIndex.class) != null;
  }
}
