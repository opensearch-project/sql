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
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.window.WindowDefinition;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class BufferPatternRowsWindowFrameTest {

  // Single partition for all rows
  @Test
  void test_single_partition_with_no_order_by() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(ImmutableList.of(), ImmutableList.of()),
            LOG_PARSER,
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(TEST_TUPLE_1, TEST_TUPLE_2, TEST_TUPLE_3));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_1, TEST_TUPLE_2, TEST_TUPLE_3), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_2, "1"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_3, "2"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertFalse(windowFrame.hasNext());
  }

  @Test
  void test_single_partition_with_different_order_by_values() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(
                ImmutableList.of(DSL.ref("level", STRING)),
                ImmutableList.of(Pair.of(DEFAULT_ASC, DSL.ref("timestamp", INTEGER)))),
            LOG_PARSER,
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(TEST_TUPLE_1, TEST_TUPLE_2, TEST_TUPLE_3));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_1, TEST_TUPLE_2), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_2, "1"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertFalse(windowFrame.hasNext());

    // Similar to PeerRowsWindowFrame concept, patterns will be grouped on peer rows level if
    // specified order by
    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_3, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_3), windowFrame.next());
    assertFalse(windowFrame.hasNext());
  }

  @Test
  void test_two_partitions_with_partition_by_and_order_by() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(
                ImmutableList.of(DSL.ref("level", STRING)),
                ImmutableList.of(Pair.of(DEFAULT_ASC, DSL.ref("timestamp", INTEGER)))),
            LOG_PARSER,
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(TEST_TUPLE_1, TEST_TUPLE_3, TEST_TUPLE_4));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_1), windowFrame.next());
    assertFalse(windowFrame.hasNext());

    // Similar to PeerRowsWindowFrame concept, patterns will be grouped on peer rows level if
    // specified order by
    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_3, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_3), windowFrame.next());
    assertFalse(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_ERROR_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_4), windowFrame.next());
    assertFalse(windowFrame.hasNext());
  }

  @Test
  void test_two_partitions_with_no_order_by() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(ImmutableList.of(DSL.ref("level", STRING)), ImmutableList.of()),
            LOG_PARSER,
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(TEST_TUPLE_1, TEST_TUPLE_3, TEST_TUPLE_5));

    // it just cares about partitions regardless of different order by values with only specified
    // partition by
    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_1, TEST_TUPLE_3), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_3, "1"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertFalse(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_ERROR_2, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_5), windowFrame.next());
    assertFalse(windowFrame.hasNext());
  }

  @Test
  void test_two_partitions_with_no_partition_by() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(
                ImmutableList.of(),
                ImmutableList.of(Pair.of(DEFAULT_ASC, DSL.ref("timestamp", INTEGER)))),
            LOG_PARSER,
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(TEST_TUPLE_1, TEST_TUPLE_4, TEST_TUPLE_5));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_1), windowFrame.next());
    assertFalse(windowFrame.hasNext());

    // Though the same partition, peer rows are still grouped by order by level
    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_ERROR_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_4, TEST_TUPLE_5), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_ERROR_2, "1"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertFalse(windowFrame.hasNext());
  }

  // Will have the same effect with single partition for all rows
  @Test
  void test_two_partitions_with_no_partition_by_and_order_by() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(ImmutableList.of(), ImmutableList.of()),
            LOG_PARSER,
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(TEST_TUPLE_1, TEST_TUPLE_4, TEST_TUPLE_5));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_1, TEST_TUPLE_4, TEST_TUPLE_5), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_ERROR_1, "1"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_ERROR_2, "2"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertFalse(windowFrame.hasNext());
  }

  @Test
  public void test_load_mixed_expr_null_value_and_string_value() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(ImmutableList.of(), ImmutableList.of()),
            LOG_PARSER,
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(Iterators.forArray(TEST_TUPLE_1, TEST_NULL_TUPLE, TEST_MISSING_TUPLE));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(TEST_TUPLE_1, TEST_NULL_TUPLE, TEST_MISSING_TUPLE), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess("", "1"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess("", "2"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertFalse(windowFrame.hasNext());
  }

  private static ExprValue tuple(String level, int timestamp, ExprValue message) {
    return fromExprValueMap(
        ImmutableMap.of(
            "level",
            new ExprStringValue(level),
            "timestamp",
            new ExprIntegerValue(timestamp),
            "message",
            message));
  }

  private static final String TEST_MESSAGE_1 =
      "12.132.31.17 - - [2018-07-22T05:36:25.812Z] \\\"GET /opensearch HTTP/1.1\\\" 200 9797"
          + " \\\"-\\\" \\\"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421"
          + " Firefox/6.0a1\\\"";
  private static final String TEST_MESSAGE_2 =
      "129.138.185.193 - - [2018-07-22T05:39:39.668Z] \\\"GET /opensearch HTTP/1.1\\\" 404 9920"
          + " \\\"-\\\" \\\"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421"
          + " Firefox/6.0a1\\\"";
  private static final String TEST_MESSAGE_3 =
      "240.58.187.246 - - [2018-07-22T06:02:46.006Z] \\\"GET /opensearch HTTP/1.1\\\" 500 6936"
          + " \\\"-\\\" \\\"Mozilla/5.0 (X11; Linux i686) AppleWebKit/534.24 (KHTML, like Gecko)"
          + " Chrome/11.0.696.50 Safari/534.24\\\"";
  private static final String TEST_ERROR_1 =
      "Unexpected exception causing shutdown while sock still open";
  private static final String TEST_ERROR_2 = "ERROR in contacting RM";
  private static final ExprValue TEST_TUPLE_1 = tuple("INFO", 10, new ExprStringValue(TEST_MESSAGE_1));
  private static final ExprValue TEST_TUPLE_2 = tuple("INFO", 10, new ExprStringValue(TEST_MESSAGE_2));
  private static final ExprValue TEST_TUPLE_3 = tuple("INFO", 15, new ExprStringValue(TEST_MESSAGE_3));
  private static final ExprValue TEST_TUPLE_4 = tuple("ERROR", 20, new ExprStringValue(TEST_ERROR_1));
  private static final ExprValue TEST_TUPLE_5 = tuple("ERROR", 20, new ExprStringValue(TEST_ERROR_2));
  private static final ExprValue TEST_NULL_TUPLE = tuple("INFO", 10, ExprNullValue.of());
  private static final ExprValue TEST_MISSING_TUPLE = tuple("INFO", 10, ExprMissingValue.of());
  private static final BrainLogParser LOG_PARSER = new BrainLogParser();
}
