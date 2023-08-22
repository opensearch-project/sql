/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.ref;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.aggregation.AggregateWindowFunction;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class WindowOperatorTest extends PhysicalPlanTestBase {

  @Test
  void test_ranking_window_function() {
    window(DSL.rank())
        .partitionBy(ref("action", STRING))
        .sortBy(DEFAULT_ASC, ref("response", INTEGER))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "209.160.24.63",
                "action",
                "GET",
                "response",
                200,
                "referer",
                "www.amazon.com",
                "rank()",
                1))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "112.111.162.4",
                "action",
                "GET",
                "response",
                200,
                "referer",
                "www.amazon.com",
                "rank()",
                1))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "209.160.24.63",
                "action",
                "GET",
                "response",
                404,
                "referer",
                "www.amazon.com",
                "rank()",
                3))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "74.125.19.106",
                "action",
                "POST",
                "response",
                200,
                "referer",
                "www.google.com",
                "rank()",
                1))
        .expectNext(
            ImmutableMap.of("ip", "74.125.19.106", "action", "POST", "response", 500, "rank()", 2))
        .done();
  }

  @SuppressWarnings("unchecked")
  @Test
  void test_aggregate_window_function() {
    window(new AggregateWindowFunction(DSL.sum(ref("response", INTEGER))))
        .partitionBy(ref("action", STRING))
        .sortBy(DEFAULT_ASC, ref("response", INTEGER))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "209.160.24.63",
                "action",
                "GET",
                "response",
                200,
                "referer",
                "www.amazon.com",
                "sum(response)",
                400))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "112.111.162.4",
                "action",
                "GET",
                "response",
                200,
                "referer",
                "www.amazon.com",
                "sum(response)",
                400))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "209.160.24.63",
                "action",
                "GET",
                "response",
                404,
                "referer",
                "www.amazon.com",
                "sum(response)",
                804))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "74.125.19.106",
                "action",
                "POST",
                "response",
                200,
                "referer",
                "www.google.com",
                "sum(response)",
                200))
        .expectNext(
            ImmutableMap.of(
                "ip", "74.125.19.106", "action", "POST", "response", 500, "sum(response)", 700))
        .done();
  }

  @SuppressWarnings("unchecked")
  @Test
  void test_aggregate_window_function_without_sort_key() {
    window(new AggregateWindowFunction(DSL.sum(ref("response", INTEGER))))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "209.160.24.63",
                "action",
                "GET",
                "response",
                200,
                "referer",
                "www.amazon.com",
                "sum(response)",
                1504))
        .expectNext(
            ImmutableMap.of(
                "ip", "74.125.19.106", "action", "POST", "response", 500, "sum(response)", 1504))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "74.125.19.106",
                "action",
                "POST",
                "response",
                200,
                "referer",
                "www.google.com",
                "sum(response)",
                1504))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "112.111.162.4",
                "action",
                "GET",
                "response",
                200,
                "referer",
                "www.amazon.com",
                "sum(response)",
                1504))
        .expectNext(
            ImmutableMap.of(
                "ip",
                "209.160.24.63",
                "action",
                "GET",
                "response",
                404,
                "referer",
                "www.amazon.com",
                "sum(response)",
                1504))
        .done();
  }

  private WindowOperatorAssertion window(Expression windowFunction) {
    return new WindowOperatorAssertion(windowFunction);
  }

  @RequiredArgsConstructor
  private static class WindowOperatorAssertion {
    private final NamedExpression windowFunction;
    private final List<Expression> partitionByList = new ArrayList<>();
    private final List<Pair<SortOption, Expression>> sortList = new ArrayList<>();

    private WindowOperator windowOperator;

    private WindowOperatorAssertion(Expression windowFunction) {
      this.windowFunction = DSL.named(windowFunction);
    }

    WindowOperatorAssertion partitionBy(Expression expr) {
      partitionByList.add(expr);
      return this;
    }

    WindowOperatorAssertion sortBy(SortOption option, Expression expr) {
      sortList.add(Pair.of(option, expr));
      return this;
    }

    WindowOperatorAssertion expectNext(Map<String, Object> expected) {
      if (windowOperator == null) {
        WindowDefinition definition = new WindowDefinition(partitionByList, sortList);
        windowOperator =
            new WindowOperator(
                new SortOperator(new TestScan(), definition.getAllSortItems()),
                windowFunction,
                definition);
        windowOperator.open();
      }

      assertTrue(windowOperator.hasNext());
      assertEquals(ExprValueUtils.tupleValue(expected), windowOperator.next());
      return this;
    }

    void done() {
      Objects.requireNonNull(windowOperator);
      assertFalse(windowOperator.hasNext());
      windowOperator.close();
    }
  }
}
