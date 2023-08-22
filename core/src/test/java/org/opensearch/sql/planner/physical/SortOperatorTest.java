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
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.sort;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort.SortOption;

@ExtendWith(MockitoExtension.class)
class SortOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  /**
   * construct the map which contain null value, because {@link ImmutableMap} doesn't support null
   * value.
   */
  private static final Map<String, Object> NULL_MAP =
      new HashMap<String, Object>() {
        {
          put("size", 399);
          put("response", null);
        }
      };

  @Test
  public void sort_one_field_asc() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    assertThat(
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
  }

  @Test
  public void sort_one_field_with_duplication() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    assertThat(
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
  }

  @Test
  public void sort_one_field_asc_with_null_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)))
        .thenReturn(tupleValue(NULL_MAP));

    assertThat(
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
  }

  @Test
  public void sort_one_field_asc_with_missing_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399)));

    assertThat(
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
  }

  @Test
  public void sort_one_field_desc() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    assertThat(
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
  }

  @Test
  public void sort_one_field_desc_with_null_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(NULL_MAP))
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    assertThat(
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(NULL_MAP)));
  }

  @Test
  public void sort_one_field_with_duplicate_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)));

    assertThat(
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503))));
  }

  @Test
  public void sort_two_fields_both_asc() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)))
        .thenReturn(tupleValue(NULL_MAP));

    assertThat(
        execute(
            sort(
                inputPlan,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
  }

  @Test
  public void sort_two_fields_both_desc() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)))
        .thenReturn(tupleValue(NULL_MAP));

    assertThat(
        execute(
            sort(
                inputPlan,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
  }

  @Test
  public void sort_two_fields_asc_and_desc() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)))
        .thenReturn(tupleValue(NULL_MAP));

    assertThat(
        execute(
            sort(
                inputPlan,
                Pair.of(SortOption.DEFAULT_ASC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_DESC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 320, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 499, "response", 404))));
  }

  @Test
  public void sort_two_fields_desc_and_asc() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, true, true, false);
    when(inputPlan.next())
        .thenReturn(tupleValue(ImmutableMap.of("size", 499, "response", 404)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 320, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 200)))
        .thenReturn(tupleValue(ImmutableMap.of("size", 399, "response", 503)))
        .thenReturn(tupleValue(NULL_MAP));

    assertThat(
        execute(
            sort(
                inputPlan,
                Pair.of(SortOption.DEFAULT_DESC, ref("size", INTEGER)),
                Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))),
        contains(
            tupleValue(ImmutableMap.of("size", 499, "response", 404)),
            tupleValue(NULL_MAP),
            tupleValue(ImmutableMap.of("size", 399, "response", 200)),
            tupleValue(ImmutableMap.of("size", 399, "response", 503)),
            tupleValue(ImmutableMap.of("size", 320, "response", 200))));
  }

  @Test
  public void sort_one_field_without_input() {
    when(inputPlan.hasNext()).thenReturn(false);

    assertEquals(
        0,
        execute(sort(inputPlan, Pair.of(SortOption.DEFAULT_ASC, ref("response", INTEGER)))).size());
  }
}
