/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.ranking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.data.model.ExprTupleValue.fromExprValueMap;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.frame.CurrentRowWindowFrame;

/** Rank window function test collection. */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class RankingWindowFunctionTest extends ExpressionTestBase {

  private final CurrentRowWindowFrame windowFrame1 =
      new CurrentRowWindowFrame(
          new WindowDefinition(
              ImmutableList.of(DSL.ref("state", STRING)),
              ImmutableList.of(Pair.of(DEFAULT_ASC, DSL.ref("age", INTEGER)))));

  private final CurrentRowWindowFrame windowFrame2 =
      new CurrentRowWindowFrame(
          new WindowDefinition(
              ImmutableList.of(DSL.ref("state", STRING)),
              ImmutableList.of())); // No sort items defined

  private PeekingIterator<ExprValue> iterator1;
  private PeekingIterator<ExprValue> iterator2;

  @BeforeEach
  void set_up() {
    iterator1 =
        Iterators.peekingIterator(
            Iterators.forArray(
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(30))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(30))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(40))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("CA"), "age", new ExprIntegerValue(20)))));

    iterator2 =
        Iterators.peekingIterator(
            Iterators.forArray(
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(30))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(30))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(50))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(55))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("CA"), "age", new ExprIntegerValue(15)))));
  }

  @Test
  void test_value_of() {
    PeekingIterator<ExprValue> iterator =
        Iterators.peekingIterator(
            Iterators.singletonIterator(
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(30)))));

    RankingWindowFunction rowNumber = DSL.rowNumber();

    windowFrame1.load(iterator);
    assertEquals(new ExprIntegerValue(1), rowNumber.valueOf(windowFrame1));
  }

  @Test
  void test_row_number() {
    RankingWindowFunction rowNumber = DSL.rowNumber();

    windowFrame1.load(iterator1);
    assertEquals(1, rowNumber.rank(windowFrame1));

    windowFrame1.load(iterator1);
    assertEquals(2, rowNumber.rank(windowFrame1));

    windowFrame1.load(iterator1);
    assertEquals(3, rowNumber.rank(windowFrame1));

    windowFrame1.load(iterator1);
    assertEquals(1, rowNumber.rank(windowFrame1));
  }

  @Test
  void test_rank() {
    RankingWindowFunction rank = DSL.rank();

    windowFrame1.load(iterator2);
    assertEquals(1, rank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(1, rank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(3, rank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(4, rank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(1, rank.rank(windowFrame1));
  }

  @Test
  void test_dense_rank() {
    RankingWindowFunction denseRank = DSL.denseRank();

    windowFrame1.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(2, denseRank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(3, denseRank.rank(windowFrame1));

    windowFrame1.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame1));
  }

  @Test
  void row_number_should_work_if_no_sort_items_defined() {
    RankingWindowFunction rowNumber = DSL.rowNumber();

    windowFrame2.load(iterator1);
    assertEquals(1, rowNumber.rank(windowFrame2));

    windowFrame2.load(iterator1);
    assertEquals(2, rowNumber.rank(windowFrame2));

    windowFrame2.load(iterator1);
    assertEquals(3, rowNumber.rank(windowFrame2));

    windowFrame2.load(iterator1);
    assertEquals(1, rowNumber.rank(windowFrame2));
  }

  @Test
  void rank_should_always_return_1_if_no_sort_items_defined() {
    PeekingIterator<ExprValue> iterator =
        Iterators.peekingIterator(
            Iterators.forArray(
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(30))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(30))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(50))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("WA"), "age", new ExprIntegerValue(55))),
                fromExprValueMap(
                    ImmutableMap.of(
                        "state", new ExprStringValue("CA"), "age", new ExprIntegerValue(15)))));

    RankingWindowFunction rank = DSL.rank();

    windowFrame2.load(iterator);
    assertEquals(1, rank.rank(windowFrame2));

    windowFrame2.load(iterator);
    assertEquals(1, rank.rank(windowFrame2));

    windowFrame2.load(iterator);
    assertEquals(1, rank.rank(windowFrame2));

    windowFrame2.load(iterator);
    assertEquals(1, rank.rank(windowFrame2));

    windowFrame2.load(iterator);
    assertEquals(1, rank.rank(windowFrame2));
  }

  @Test
  void dense_rank_should_always_return_1_if_no_sort_items_defined() {
    RankingWindowFunction denseRank = DSL.denseRank();

    windowFrame2.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame2));

    windowFrame2.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame2));

    windowFrame2.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame2));

    windowFrame2.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame2));

    windowFrame2.load(iterator2);
    assertEquals(1, denseRank.rank(windowFrame2));
  }
}
