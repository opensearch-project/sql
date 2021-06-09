/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;

public class AggregatorStateTest extends AggregationTest {

  @Test
  void count_distinct_values() {
    CountAggregator.CountState state = new CountAggregator.CountState();
    state.count(new ExprIntegerValue(1));
    assertFalse(state.distinctValues().isEmpty());
  }

  @Test
  void default_distinct_values() {
    AvgAggregator.AvgState state = new AvgAggregator.AvgState();
    assertTrue(state.distinctValues().isEmpty());
  }
}
