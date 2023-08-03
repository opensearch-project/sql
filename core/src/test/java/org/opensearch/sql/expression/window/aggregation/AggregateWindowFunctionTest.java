/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprTupleValue.fromExprValueMap;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.window.frame.PeerRowsWindowFrame;

/** Aggregate window function test collection. */
@SuppressWarnings("unchecked")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class AggregateWindowFunctionTest extends ExpressionTestBase {

  @SuppressWarnings("rawtypes")
  @Test
  void test_delegated_methods() {
    Aggregator aggregator = mock(Aggregator.class);
    when(aggregator.type()).thenReturn(LONG);
    when(aggregator.accept(any(), any())).thenReturn(123);
    when(aggregator.toString()).thenReturn("avg(age)");

    AggregateWindowFunction windowFunction = new AggregateWindowFunction(aggregator);
    assertEquals(LONG, windowFunction.type());
    assertEquals(123, (Integer) windowFunction.accept(null, null));
    assertEquals("avg(age)", windowFunction.toString());
  }

  @Test
  void should_accumulate_all_peer_values_and_not_reset_state_if_same_partition() {
    PeerRowsWindowFrame windowFrame = mock(PeerRowsWindowFrame.class);
    AggregateWindowFunction windowFunction =
        new AggregateWindowFunction(DSL.sum(DSL.ref("age", INTEGER)));

    when(windowFrame.isNewPartition()).thenReturn(true);
    when(windowFrame.next())
        .thenReturn(
            ImmutableList.of(
                fromExprValueMap(ImmutableMap.of("age", new ExprIntegerValue(10))),
                fromExprValueMap(ImmutableMap.of("age", new ExprIntegerValue(20)))));
    assertEquals(new ExprIntegerValue(30), windowFunction.valueOf(windowFrame));

    when(windowFrame.isNewPartition()).thenReturn(false);
    when(windowFrame.next())
        .thenReturn(
            ImmutableList.of(fromExprValueMap(ImmutableMap.of("age", new ExprIntegerValue(30)))));
    assertEquals(new ExprIntegerValue(60), windowFunction.valueOf(windowFrame));
  }
}
