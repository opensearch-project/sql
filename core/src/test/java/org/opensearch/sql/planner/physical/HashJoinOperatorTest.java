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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.join.HashJoinOperator;
import org.opensearch.sql.planner.physical.join.JoinOperator;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class HashJoinOperatorTest extends PhysicalPlanTestBase {
  private final JoinOperator.BuildSide defaultBuildSide = JoinOperator.BuildSide.BuildRight;
  private final Optional<Expression> defaultNonEquiCond =
      Optional.of(
          DSL.and(
              DSL.equal(DSL.ref("host", STRING), DSL.literal("h1")),
              DSL.lte(DSL.ref("id", INTEGER), DSL.literal(5))));

  @Test
  public void inner_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.INNER,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2,
                    "id",
                    2,
                    "name",
                    "b")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3, "id", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h2",
                    "errors",
                    10,
                    "id",
                    10,
                    "name",
                    "j")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-07"),
                    "host",
                    "h1",
                    "errors",
                    6,
                    "id",
                    6,
                    "name",
                    "f")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8, "id", 8))));
  }

  @Test
  public void inner_join_side_exchange_test() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("id", INTEGER)),
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            Join.JoinType.INNER,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    1,
                    "name",
                    "a",
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    1,
                    "name",
                    "a",
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    2,
                    "name",
                    "b",
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id", 3, "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    6,
                    "name",
                    "f",
                    "day",
                    new ExprDateValue("2021-01-07"),
                    "host",
                    "h1",
                    "errors",
                    6)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id", 8, "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    10,
                    "name",
                    "j",
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h2",
                    "errors",
                    10))));
  }

  @Test
  public void inner_join_with_non_equi_cond_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.INNER,
            defaultBuildSide,
            left,
            right,
            defaultNonEquiCond);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2,
                    "id",
                    2,
                    "name",
                    "b")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a"))));
  }

  @Test
  public void left_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.LEFT,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(9, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2,
                    "id",
                    2,
                    "name",
                    "b")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h2",
                    "errors",
                    10,
                    "id",
                    10,
                    "name",
                    "j")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-07"),
                    "host",
                    "h1",
                    "errors",
                    6,
                    "id",
                    6,
                    "name",
                    "f")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3, "id", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8, "id", 8)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 12)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-08"), "host", "h1", "errors", 13))));
  }

  @Test
  public void left_join_side_exchange_test() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("id", INTEGER)),
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            Join.JoinType.LEFT,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(12, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    1,
                    "name",
                    "a",
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    1,
                    "name",
                    "a",
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    2,
                    "name",
                    "b",
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id", 3, "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3)),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 4, "name", "d")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 5, "name", "e")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    6,
                    "name",
                    "f",
                    "day",
                    new ExprDateValue("2021-01-07"),
                    "host",
                    "h1",
                    "errors",
                    6)),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 7, "name", "g")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id", 8, "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8)),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 9, "name", "i")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    10,
                    "name",
                    "j",
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h2",
                    "errors",
                    10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 11, "name", "k"))));
  }

  @Test
  public void left_join_with_non_equi_cond_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.INNER,
            defaultBuildSide,
            left,
            right,
            defaultNonEquiCond);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2,
                    "id",
                    2,
                    "name",
                    "b")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a"))));
  }

  @Test
  public void right_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.RIGHT,
            JoinOperator.BuildSide.BuildLeft,
            left,
            right,
            Optional.empty());
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(12, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2,
                    "id",
                    2,
                    "name",
                    "b")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3, "id", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-07"),
                    "host",
                    "h1",
                    "errors",
                    6,
                    "id",
                    6,
                    "name",
                    "f")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8, "id", 8)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h2",
                    "errors",
                    10,
                    "id",
                    10,
                    "name",
                    "j")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 4, "name", "d")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 5, "name", "e")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 7, "name", "g")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 9, "name", "i")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 11, "name", "k"))));
  }

  @Test
  public void right_join_side_exchange_test() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("id", INTEGER)),
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            Join.JoinType.RIGHT,
            JoinOperator.BuildSide.BuildLeft,
            left,
            right,
            Optional.empty());
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(9, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    2,
                    "name",
                    "b",
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id", 3, "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    1,
                    "name",
                    "a",
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    10,
                    "name",
                    "j",
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h2",
                    "errors",
                    10)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    1,
                    "name",
                    "a",
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id",
                    6,
                    "name",
                    "f",
                    "day",
                    new ExprDateValue("2021-01-07"),
                    "host",
                    "h1",
                    "errors",
                    6)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "id", 8, "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 12)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day", new ExprDateValue("2021-01-08"), "host", "h1", "errors", 13))));
  }

  @Test
  public void right_join_with_non_equi_cond_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.RIGHT,
            JoinOperator.BuildSide.BuildLeft,
            left,
            right,
            defaultNonEquiCond);
    List<ExprValue> result = execute(joinPlan);
    result.forEach(System.out::println);
    assertEquals(12, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-04"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-06"),
                    "host",
                    "h1",
                    "errors",
                    1,
                    "id",
                    1,
                    "name",
                    "a")),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "day",
                    new ExprDateValue("2021-01-03"),
                    "host",
                    "h1",
                    "errors",
                    2,
                    "id",
                    2,
                    "name",
                    "b")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 3)),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 4, "name", "d")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 5, "name", "e")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 6, "name", "f")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 7, "name", "g")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 8)),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 9, "name", "i")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 10, "name", "j")),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 11, "name", "k"))));
  }

  @Test
  public void semi_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.SEMI,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
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
  public void semi_join_side_exchange_test() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("id", INTEGER)),
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            Join.JoinType.SEMI,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
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
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 3)),
            ExprValueUtils.tupleValue(ImmutableMap.of("id", 8))));
  }

  @Test
  public void semi_join_non_equi_cond_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.SEMI,
            defaultBuildSide,
            left,
            right,
            defaultNonEquiCond);
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
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.ANTI,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
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
  public void anti_join_side_exchange_test() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("id", INTEGER)),
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            Join.JoinType.ANTI,
            defaultBuildSide,
            left,
            right,
            Optional.empty());
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
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new HashJoinOperator(
            ImmutableList.of(DSL.ref("errors", INTEGER)),
            ImmutableList.of(DSL.ref("id", INTEGER)),
            Join.JoinType.ANTI,
            defaultBuildSide,
            left,
            right,
            defaultNonEquiCond);
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
