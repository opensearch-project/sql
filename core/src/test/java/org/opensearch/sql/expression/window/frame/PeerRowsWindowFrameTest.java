/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.frame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.data.model.ExprTupleValue.fromExprValueMap;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.window.WindowDefinition;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class PeerRowsWindowFrameTest {

  private final PeerRowsWindowFrame windowFrame =
      new PeerRowsWindowFrame(
          new WindowDefinition(
              ImmutableList.of(DSL.ref("state", STRING)),
              ImmutableList.of(Pair.of(DEFAULT_ASC, DSL.ref("age", INTEGER)))));

  @Test
  void test_single_row() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.singletonIterator(tuple("WA", 10, 100)));
    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100)), windowFrame.next());
  }

  @Test
  void test_single_partition_with_no_more_rows_after_peers() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(
            Iterators.forArray(tuple("WA", 10, 100), tuple("WA", 20, 200), tuple("WA", 20, 50)));

    // Here we simulate how WindowFrame interacts with WindowOperator which calls load()
    // and WindowFunction which calls isNewPartition() and move()
    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100)), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 20, 200), tuple("WA", 20, 50)), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(), windowFrame.next());
  }

  @Test
  void test_single_partition_with_more_rows_after_peers() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(
            Iterators.forArray(
                tuple("WA", 10, 100),
                tuple("WA", 20, 200),
                tuple("WA", 20, 50),
                tuple("WA", 35, 150)));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100)), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 20, 200), tuple("WA", 20, 50)), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 35, 150)), windowFrame.next());
  }

  @Test
  void test_two_partitions_with_all_same_peers_in_second_partition() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(
            Iterators.forArray(tuple("WA", 10, 100), tuple("CA", 18, 150), tuple("CA", 18, 100)));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100)), windowFrame.next());

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("CA", 18, 150), tuple("CA", 18, 100)), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(), windowFrame.next());
  }

  @Test
  void test_two_partitions_with_single_row_in_each_partition() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(tuple("WA", 10, 100), tuple("CA", 30, 200)));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100)), windowFrame.next());

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("CA", 30, 200)), windowFrame.next());
  }

  @Test
  void test_window_definition_with_no_partition_by() {
    PeerRowsWindowFrame windowFrame =
        new PeerRowsWindowFrame(
            new WindowDefinition(
                ImmutableList.of(),
                ImmutableList.of(Pair.of(DEFAULT_ASC, DSL.ref("age", INTEGER)))));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(tuple("WA", 10, 100), tuple("CA", 30, 200)));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100)), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("CA", 30, 200)), windowFrame.next());
  }

  @Test
  void test_window_definition_with_no_order_by() {
    PeerRowsWindowFrame windowFrame =
        new PeerRowsWindowFrame(
            new WindowDefinition(ImmutableList.of(DSL.ref("state", STRING)), ImmutableList.of()));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(tuple("WA", 10, 100), tuple("CA", 30, 200)));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100)), windowFrame.next());

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("CA", 30, 200)), windowFrame.next());
  }

  @Test
  void test_window_definition_with_no_partition_by_and_order_by() {
    PeerRowsWindowFrame windowFrame =
        new PeerRowsWindowFrame(new WindowDefinition(ImmutableList.of(), ImmutableList.of()));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(tuple("WA", 10, 100), tuple("CA", 30, 200)));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(tuple("WA", 10, 100), tuple("CA", 30, 200)), windowFrame.next());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(ImmutableList.of(), windowFrame.next());
  }

  private ExprValue tuple(String state, int age, int balance) {
    return fromExprValueMap(
        ImmutableMap.of(
            "state", new ExprStringValue(state),
            "age", new ExprIntegerValue(age),
            "balance", new ExprIntegerValue(balance)));
  }
}
