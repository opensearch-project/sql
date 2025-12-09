/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.logical.LogicalAggregate;

@UtilityClass
public class PPLHintStrategyTable {

  private static final Supplier<HintStrategyTable> HINT_STRATEGY_TABLE =
      Suppliers.memoize(
          () ->
              HintStrategyTable.builder()
                  .hintStrategy(
                      "stats_args",
                      (hint, rel) -> {
                        return rel instanceof LogicalAggregate;
                      })
                  // add more here
                  .build());

  /** Update the HINT_STRATEGY_TABLE when you create a new hint. */
  public static HintStrategyTable getHintStrategyTable() {
    return HINT_STRATEGY_TABLE.get();
  }
}
