/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ClusterLabelAggFunctionTest {

  private static final double THRESHOLD = 0.8;
  private static final String MATCH = "termlist";
  private static final String DELIMS = " ";
  private static final int BUFFER = 50000;
  private static final int MAX_CLUSTERS = 10000;

  private ClusterLabelAggFunction.Acc feed(int rows, int maxInputRows) {
    ClusterLabelAggFunction fn = new ClusterLabelAggFunction();
    ClusterLabelAggFunction.Acc acc = fn.init();
    for (int i = 0; i < rows; i++) {
      acc = fn.add(acc, "row " + i, THRESHOLD, MATCH, DELIMS, BUFFER, MAX_CLUSTERS, maxInputRows);
    }
    return acc;
  }

  @Test
  void underLimitProducesOneLabelPerRow() {
    ClusterLabelAggFunction fn = new ClusterLabelAggFunction();
    ClusterLabelAggFunction.Acc acc = feed(5, 10);
    @SuppressWarnings("unchecked")
    List<Integer> labels = (List<Integer>) fn.result(acc);
    assertEquals(5, labels.size());
  }

  @Test
  void exceedingMaxInputRowsFailsLoud() {
    // ceiling = 3, feeding a 4th row must reject rather than silently drop it.
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> feed(4, 3));
    assertTrue(ex.getMessage().contains("plugins.ppl.cluster.max.input.rows"));
    assertTrue(ex.getMessage().contains("3"));
  }

  @Test
  void exactlyAtLimitIsAllowed() {
    ClusterLabelAggFunction fn = new ClusterLabelAggFunction();
    ClusterLabelAggFunction.Acc acc = feed(3, 3);
    @SuppressWarnings("unchecked")
    List<Integer> labels = (List<Integer>) fn.result(acc);
    assertEquals(3, labels.size());
  }
}
