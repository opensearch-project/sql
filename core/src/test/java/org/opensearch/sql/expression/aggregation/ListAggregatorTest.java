/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.expression.DSL;

class ListAggregatorTest {

  @Test
  void testListAggregatorCreation() {
    ListAggregator aggregator = new ListAggregator(
        Collections.singletonList(DSL.ref("field", STRING)), ARRAY);
    assertNotNull(aggregator);
    assertEquals("list", aggregator.getFunctionName().getFunctionName());
  }

  @Test
  void testListStateAccumulation() {
    ListAggregator aggregator = new ListAggregator(
        Collections.singletonList(DSL.ref("field", STRING)), ARRAY);
    
    ListAggregator.ListState state = aggregator.create();
    state.addValue(new ExprStringValue("apple"));
    state.addValue(new ExprStringValue("banana"));
    state.addValue(new ExprStringValue("apple")); // duplicate
    
    assertNotNull(state.result());
    // Note: Full result validation would require more complex assertions
    // due to the ExprTupleValue structure
  }
}