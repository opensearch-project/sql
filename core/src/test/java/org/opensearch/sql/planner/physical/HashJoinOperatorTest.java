/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.join.JoinOperator;

@ExtendWith(MockitoExtension.class)
public class HashJoinOperatorTest extends JoinOperatorTestHelper {
  private final Optional<Expression> emptyNonEquiCond = Optional.empty();
  private final Optional<Expression> defaultNonEquiCond =
      Optional.of(
          DSL.and(
              DSL.equal(DSL.ref("error_t.host", STRING), DSL.literal("h1")),
              DSL.lte(DSL.ref("name_t.id", INTEGER), DSL.literal(5))));

  @Test
  public void inner_join_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.INNER, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            error1_id1,
            error1_id1_duplicated,
            error2_id2,
            error3_id3,
            error6_id6,
            error8_id8,
            error10_id10));
  }

  @Test
  public void inner_join_side_reversed_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.INNER, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, true);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            id1_error1,
            id1_error1_duplicated,
            id2_error2,
            id3_error3,
            id6_error6,
            id8_error8,
            id10_error10));
  }

  @Test
  public void inner_join_with_non_equi_cond_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.INNER, JoinOperator.BuildSide.BuildRight, defaultNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(3, result.size());
    assertThat(result, containsInAnyOrder(error1_id1, error1_id1_duplicated, error2_id2));
  }

  @Test
  public void left_join_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.LEFT, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(9, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            error1_id1,
            error1_id1_duplicated,
            error2_id2,
            error3_id3,
            error6_id6,
            error8_id8,
            error10_id10,
            error12_null,
            error13_null));
  }

  @Test
  public void left_join_side_reversed_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(Join.JoinType.LEFT, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, true);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(12, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            id1_error1,
            id1_error1_duplicated,
            id2_error2,
            id3_error3,
            id6_error6,
            id8_error8,
            id10_error10,
            id4_null,
            id5_null,
            id7_null,
            id9_null,
            id11_null));
  }

  @Test
  public void left_join_with_non_equi_cond_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.LEFT, JoinOperator.BuildSide.BuildRight, defaultNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(9, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            error1_id1,
            error1_id1_duplicated,
            error2_id2,
            error3_null,
            error6_null,
            error8_null,
            error10_null,
            error12_null,
            error13_null));
  }

  @Test
  public void right_join_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.RIGHT, JoinOperator.BuildSide.BuildLeft, emptyNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(12, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            error1_id1,
            error1_id1_duplicated,
            error2_id2,
            error3_id3,
            error6_id6,
            error8_id8,
            error10_id10,
            null_id4,
            null_id5,
            null_id7,
            null_id9,
            null_id11));
  }

  @Test
  public void right_join_side_reversed_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(Join.JoinType.RIGHT, JoinOperator.BuildSide.BuildLeft, emptyNonEquiCond, true);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(9, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            id1_error1,
            id1_error1_duplicated,
            id2_error2,
            id3_error3,
            id6_error6,
            id8_error8,
            id10_error10,
            null_error12,
            null_error13));
  }

  @Test
  public void right_join_with_non_equi_cond_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.RIGHT, JoinOperator.BuildSide.BuildLeft, defaultNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(12, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            error1_id1,
            error1_id1_duplicated,
            error2_id2,
            null_id3,
            null_id4,
            null_id5,
            null_id6,
            null_id7,
            null_id8,
            null_id9,
            null_id10,
            null_id11));
  }

  @Test
  public void semi_join_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.SEMI, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-04"), "host", "h1", "errors", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-06"), "host", "h1", "errors", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-03"), "host", "h1", "errors", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-07"), "host", "h1", "errors", 6)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-04"), "host", "h2", "errors", 10))));
  }

  @Test
  public void semi_join_side_reversed_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(Join.JoinType.SEMI, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, true);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(6, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 2, "name", "b")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 10, "name", "j")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 1, "name", "a")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 6, "name", "f")),
            ExprValueUtils.tupleValue(
                new LinkedHashMap<>() {
                  {
                    put("id", 3);
                    put("name", null);
                  }
                }),
            ExprValueUtils.tupleValue(
                new LinkedHashMap<>() {
                  {
                    put("id", 8);
                    put("name", null);
                  }
                })));
  }

  @Test
  public void semi_join_non_equi_cond_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.SEMI, JoinOperator.BuildSide.BuildRight, defaultNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-04"), "host", "h1", "errors", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-06"), "host", "h1", "errors", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-03"), "host", "h1", "errors", 2))));
  }

  @Test
  public void anti_join_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.ANTI, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 12)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-08"), "host", "h1", "errors", 13))));
  }

  @Test
  public void anti_join_side_reversed_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(Join.JoinType.ANTI, JoinOperator.BuildSide.BuildRight, emptyNonEquiCond, true);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(5, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 4, "name", "d")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 5, "name", "e")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 7, "name", "g")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 9, "name", "i")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 11, "name", "k"))));
  }

  @Test
  public void anti_join_non_equi_cond_test() {
    PhysicalPlan joinPlan =
        makeHashJoin(
            Join.JoinType.ANTI, JoinOperator.BuildSide.BuildRight, defaultNonEquiCond, false);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(6, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-07"), "host", "h1", "errors", 6)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-04"), "host", "h2", "errors", 10)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 12)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-08"), "host", "h1", "errors", 13))));
  }
}
