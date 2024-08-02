/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.limit;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.sort;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.takeOrdered;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprValue;

/**
 * To make sure {@link TakeOrderedOperator} can replace {@link SortOperator} + {@link
 * LimitOperator}, this UT will replica all tests in {@link SortOperatorTest} and add more test
 * cases on different limit and offset.
 */
@ExtendWith(MockitoExtension.class)
class TakeOrderedOperatorTest extends PhysicalPlanTestBase {
  private static PhysicalPlan inputPlan;

  @Getter
  @Setter
  private static class Wrapper {
    Iterator<ExprValue> iterator = Collections.emptyIterator();
  }

  private static final Wrapper wrapper = new Wrapper();

  @BeforeAll
  public static void setUp() {
    inputPlan = Mockito.mock(PhysicalPlan.class);
    when(inputPlan.hasNext())
        .thenAnswer((InvocationOnMock invocation) -> wrapper.iterator.hasNext());
    when(inputPlan.next()).thenAnswer((InvocationOnMock invocation) -> wrapper.iterator.next());
  }

  /**
   * construct the map which contain null value, because {@link ImmutableMap} doesn't support null
   * value.
   */
  private static final Map<String, Object> NULL_MAP =
      new HashMap<>() {
        {
          put("size", 399);
          put("response", null);
        }
      };

  @Test
  public void sort_one_field_asc() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 2, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 2, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 2, 1, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 2, 1);

    wrapper.setIterator(inputList.iterator());
    assertEquals(
        0,
        execute(
            takeOrdered(
                inputPlan, 0, 1, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))).size());
    compare_takeOrdered_with_sort_limit(inputList, 0, 1);
  }

  @Test
  public void sort_one_field_with_duplication() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 2, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 2, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 2, 1, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 2, 1);
  }

  @Test
  public void sort_one_field_asc_with_null_value() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(NULL_MAP));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 4, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 1, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 1);
  }

  @Test
  public void sort_one_field_asc_with_missing_value() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399)));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 4, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 1, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 1);
  }

  @Test
  public void sort_one_field_desc() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 0, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 2, 0, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 2, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 2, 1, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
    compare_takeOrdered_with_sort_limit(inputList, 2, 1);
  }

  @Test
  public void sort_one_field_desc_with_null_value() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 4, 0, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(NULL_MAP)));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 0, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 1, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(NULL_MAP)));
    compare_takeOrdered_with_sort_limit(inputList, 3, 1);
  }

  @Test
  public void sort_one_field_with_duplicate_value() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 4, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan, 3, 1, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 3, 1);
  }

  @Test
  public void sort_two_fields_both_asc() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(NULL_MAP));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                5,
                0,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 5, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                0,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                1,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 1);
  }

  @Test
  public void sort_two_fields_both_desc() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(NULL_MAP));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                5,
                0,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
    compare_takeOrdered_with_sort_limit(inputList, 5, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                0,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP)));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                1,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 1);
  }

  @Test
  public void sort_two_fields_asc_and_desc() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(NULL_MAP));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                5,
                0,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 5, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                0,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP)));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                1,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 1);
  }

  @Test
  public void sort_two_fields_desc_and_asc() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(NULL_MAP));

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                5,
                0,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
    compare_takeOrdered_with_sort_limit(inputList, 5, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                0,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 0);

    wrapper.setIterator(inputList.iterator());
    assertThat(
        execute(
            takeOrdered(
                inputPlan,
                4,
                1,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
    compare_takeOrdered_with_sort_limit(inputList, 4, 1);
  }

  @Test
  public void sort_one_field_without_input() {
    wrapper.setIterator(Collections.emptyIterator());
    assertEquals(
        0,
        execute(
                takeOrdered(
                    inputPlan, 1, 0, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER))))
            .size());
  }

  @Test
  public void offset_exceeds_row_number() {
    List<ExprValue> inputList =
        Arrays.asList(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(NULL_MAP));

    wrapper.setIterator(inputList.iterator());
    PhysicalPlan plan =
        takeOrdered(inputPlan, 1, 6, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)));
    List<ExprValue> result = execute(plan);
    assertEquals(0, result.size());
  }

  private void compare_takeOrdered_with_sort_limit(List<ExprValue> inputList, int limit, int offset) {
    wrapper.setIterator(inputList.iterator());
    List<ExprValue> expectedResult = execute(limit(sort(
        inputPlan, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER))), limit, offset));
    wrapper.setIterator(inputList.iterator());
    List<ExprValue> actualResult = execute(
        takeOrdered(
            inputPlan, limit, offset, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER))));
    assertEquals(expectedResult, actualResult);
  }
}
