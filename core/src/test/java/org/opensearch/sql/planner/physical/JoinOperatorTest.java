/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import com.google.common.collect.ImmutableMap;
import java.util.List;
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
import org.opensearch.sql.planner.physical.join.NestedLoopJoinOperator;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class JoinOperatorTest extends PhysicalPlanTestBase {

  @Test
  public void nested_loop_inner_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.INNER,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_inner_join_test_2() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.INNER,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_left_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.LEFT,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_left_join_test_2() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.LEFT,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_right_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.RIGHT,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_right_join_test_2() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.RIGHT,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_semi_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.SEMI,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_semi_join_test_2() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.SEMI,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_anti_join_test() {
    PhysicalPlan left = testScan(joinTestInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.ANTI,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
  public void nested_loop_anti_join_test_2() {
    // Exchange the tables
    PhysicalPlan left = testScan(countTestInputs);
    PhysicalPlan right = testScan(joinTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.ANTI,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
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
}
