/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.opensearch.sql.common.setting.Settings;

/**
 * ClickBench SQL functional query compatibility test with Calcite engine enabled.
 *
 * <p>Runs the same 43 ClickBench queries as {@link SQLClickBenchIT} but with the Calcite query
 * engine enabled. This validates Calcite's ability to handle the analytical SQL patterns used in
 * ClickHouse workloads, which is the foundation for the ClickHouse dialect support.
 */
@FixMethodOrder(MethodSorters.JVM)
public class CalciteSQLClickBenchIT extends SQLClickBenchIT {

  @Override
  public void init() throws Exception {
    super.init();
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "true"));
  }

  /**
   * With Calcite enabled, fewer queries need to be skipped since Calcite supports
   * REGEXP_REPLACE and DATE_TRUNC natively.
   */
  @Override
  protected Set<Integer> ignored() {
    Set<Integer> ignored = new HashSet<>();
    ignored.add(30); // high memory consumption
    ignored.add(35); // GROUP BY ordinal
    return ignored;
  }
}
