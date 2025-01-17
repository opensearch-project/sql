/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.project;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.utils.TestOperator;

@ExtendWith(MockitoExtension.class)
class ProjectOperatorTest extends PhysicalPlanTestBase {

  @Mock(serializable = true)
  private PhysicalPlan inputPlan;

  @Test
  public void project_one_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200)));
    PhysicalPlan plan = project(inputPlan, DSL.named("action", DSL.ref("action", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET")))));
  }

  @Test
  public void project_two_field_follow_the_project_order() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200)));
    PhysicalPlan plan =
        project(
            inputPlan,
            DSL.named("response", DSL.ref("response", INTEGER)),
            DSL.named("action", DSL.ref("action", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("response", 200, "action", "GET")))));
  }

  @Test
  public void project_keep_missing_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", 200)))
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST")));
    PhysicalPlan plan =
        project(
            inputPlan,
            DSL.named("response", DSL.ref("response", INTEGER)),
            DSL.named("action", DSL.ref("action", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(2),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("response", 200, "action", "GET")),
                ExprTupleValue.fromExprValueMap(
                    ImmutableMap.of("response", LITERAL_MISSING, "action", stringValue("POST"))))));
  }

  @Test
  public void project_schema() {
    PhysicalPlan project =
        project(
            inputPlan,
            DSL.named("response", DSL.ref("response", INTEGER)),
            DSL.named("act", DSL.ref("action", STRING)));

    assertThat(
        project.schema().getColumns(),
        contains(
            new ExecutionEngine.Schema.Column("response", "response", INTEGER),
            new ExecutionEngine.Schema.Column("action", "act", STRING)));
  }

  @Test
  public void project_fields_with_parse_expressions() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200")));
    PhysicalPlan plan =
        project(
            inputPlan,
            ImmutableList.of(
                DSL.named("action", DSL.ref("action", STRING)),
                DSL.named("response", DSL.ref("response", STRING))),
            ImmutableList.of(
                DSL.named(
                    "action",
                    DSL.regex(
                        DSL.ref("response", STRING),
                        DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                        DSL.literal("action"))),
                DSL.named(
                    "response",
                    DSL.regex(
                        DSL.ref("response", STRING),
                        DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                        DSL.literal("response")))));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", "200")))));
  }

  @Test
  public void project_fields_with_unused_parse_expressions() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200")));
    PhysicalPlan plan =
        project(
            inputPlan,
            ImmutableList.of(DSL.named("response", DSL.ref("response", STRING))),
            ImmutableList.of(
                DSL.named(
                    "ignored",
                    DSL.regex(
                        DSL.ref("response", STRING),
                        DSL.literal("(?<action>\\w+) (?<ignored>\\d+)"),
                        DSL.literal("ignored")))));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200")))));
  }

  @Test
  public void project_fields_with_parse_expressions_and_runtime_fields() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("response", "GET 200", "eval_field", 1)));
    PhysicalPlan plan =
        project(
            inputPlan,
            ImmutableList.of(
                DSL.named("response", DSL.ref("response", STRING)),
                DSL.named("action", DSL.ref("action", STRING)),
                DSL.named("eval_field", DSL.ref("eval_field", INTEGER))),
            ImmutableList.of(
                DSL.named(
                    "action",
                    DSL.regex(
                        DSL.ref("response", STRING),
                        DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                        DSL.literal("action")))));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(
                    ImmutableMap.of("response", "GET 200", "action", "GET", "eval_field", 1)))));
  }

  @Test
  public void project_parse_missing_will_fallback() {
    when(inputPlan.hasNext()).thenReturn(true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", "GET 200")))
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST")));
    PhysicalPlan plan =
        project(
            inputPlan,
            ImmutableList.of(
                DSL.named("action", DSL.ref("action", STRING)),
                DSL.named("response", DSL.ref("response", STRING))),
            ImmutableList.of(
                DSL.named(
                    "action",
                    DSL.regex(
                        DSL.ref("response", STRING),
                        DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                        DSL.literal("action"))),
                DSL.named(
                    "response",
                    DSL.regex(
                        DSL.ref("response", STRING),
                        DSL.literal("(?<action>\\w+) (?<response>\\d+)"),
                        DSL.literal("response")))));
    List<ExprValue> result = execute(plan);

    assertThat(
        result,
        allOf(
            iterableWithSize(2),
            hasItems(
                ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "response", "200")),
                ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST")))));
  }

  @Test
  @SneakyThrows
  public void serializable() {
    var projects = List.of(DSL.named("action", DSL.ref("action", STRING)));
    var project = new ProjectOperator(new TestOperator(), projects, List.of());

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeObject(project);
    objectOutput.flush();

    ObjectInputStream objectInput =
        new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()));
    var roundTripPlan = (ProjectOperator) objectInput.readObject();
    assertEquals(project, roundTripPlan);
  }
}
