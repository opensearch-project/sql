/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;

/** {@link ScanAggregates#moreThanOneGroupedOverAScan} on plan shapes the PPL tests cannot build. */
class ScanAggregatesTest {

  @Test
  void failsClosedWhenThePlanCannotBeWalked() {
    RelNode broken = mock(RelNode.class);
    when(broken.getInputs()).thenThrow(new IllegalStateException("no inputs for you"));

    assertTrue(
        ScanAggregates.moreThanOneGroupedOverAScan(broken),
        "an unwalkable plan must bar partial results");
  }

  @Test
  void ungroupedAggregatesDoNotCount() {
    // addcoltotals' totals row and `stats sum(c)` over a stats: nothing to partition on.
    RelNode grouped = aggregateOver(scan());

    assertFalse(
        ScanAggregates.moreThanOneGroupedOverAScan(
            aggregateOver(relay(grouped), ImmutableBitSet.of())));
  }

  @Test
  void countsASharedAggregateOnce() {
    // chart hands one aggregate to both its data branch and its ranking branch.
    RelNode shared = aggregateOver(scan());

    assertFalse(ScanAggregates.moreThanOneGroupedOverAScan(fork(shared, relay(shared))));
  }

  /** An aggregate over a source with no scan, e.g. makeresults, narrows nothing. */
  @Test
  void aggregatesWithoutAScanDoNotCount() {
    RelNode scanless = relay(stub(cheapMock(RelNode.class), List.of()));

    assertFalse(
        ScanAggregates.moreThanOneGroupedOverAScan(
            fork(aggregateOver(scanless), aggregateOver(scanless))));
  }

  @Test
  void walksDeepPlansWithoutOverflowing() {
    RelNode deep = scan();
    for (int i = 0; i < 20_000; i++) {
      deep = relay(deep);
    }

    assertFalse(ScanAggregates.moreThanOneGroupedOverAScan(aggregateOver(deep)));
    assertTrue(
        ScanAggregates.moreThanOneGroupedOverAScan(fork(aggregateOver(deep), aggregateOver(deep))));
  }

  private static RelNode scan() {
    return stub(cheapMock(TableScan.class), List.of());
  }

  /** A node that neither aggregates nor scans, e.g. a project or a filter. */
  private static RelNode relay(RelNode input) {
    return stub(cheapMock(RelNode.class), List.of(input));
  }

  private static RelNode fork(RelNode left, RelNode right) {
    return stub(cheapMock(RelNode.class), List.of(left, right));
  }

  private static RelNode aggregateOver(RelNode input) {
    return aggregateOver(input, ImmutableBitSet.of(0));
  }

  private static RelNode aggregateOver(RelNode input, ImmutableBitSet groupSet) {
    Aggregate aggregate = cheapMock(Aggregate.class);
    when(aggregate.getInput()).thenReturn(input);
    when(aggregate.getGroupSet()).thenReturn(groupSet);
    return stub(aggregate, List.of(input));
  }

  /** stubOnly: recording 20k mocks is slow enough to matter here. */
  private static <T extends RelNode> T cheapMock(Class<T> type) {
    return mock(type, withSettings().stubOnly());
  }

  private static <T extends RelNode> T stub(T node, List<RelNode> inputs) {
    when(node.getInputs()).thenReturn(inputs);
    when(node.accept(any(RexShuttle.class))).thenReturn(node);
    return node;
  }
}
