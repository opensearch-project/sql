/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.parse;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
public class ParseOperatorTest extends PhysicalPlanTestBase {
  @Mock
  private PhysicalPlan inputPlan;

  @Test
  public void parseTest() {
    PhysicalPlan plan = parse(new TestScan(), DSL.ref("ip", STRING),
        "(?<ipMatched>^(\\d{1,3}\\.){3}\\d{1,3}$)", ImmutableMap.of("ipMatched", ""));
    List<ExprValue> results = execute(plan);
    assertEquals(5, results.size());
    results.forEach(result -> assertEquals(result.tupleValue().get("ip"),
        result.tupleValue().get("ipMatched")));
  }

  @Test
  public void parseAndCastMultipleFieldsTest() {
    PhysicalPlan plan = parse(new TestScan(), DSL.ref("ip", STRING),
        "(?<num0INTEGER>\\d{1,3})\\.(?<num1INTEGER>\\d{1,3})\\.(?<num2INTEGER>\\d{1,3})"
            + "\\.(?<num3INTEGER>\\d{1,3})",
        ImmutableMap.of("num0", "INTEGER", "num1", "INTEGER", "num2", "INTEGER", "num3",
            "INTEGER"));
    List<ExprValue> results = execute(plan);
    assertEquals(5, results.size());
    PhysicalPlanTestBase.inputs.stream().map(
        input -> Arrays.stream(input.tupleValue().get("ip").stringValue().split("\\."))
            .map(Integer::valueOf).collect(Collectors.toList()))
        .forEach(ip -> IntStream.range(0, results.size())
            .allMatch(i -> IntStream.range(0, 3)
                .allMatch(j -> results.get(i).tupleValue().get("num" + j).equals(ip.get(j)))));
  }

  @Test
  public void parseReplaceExistingFieldTest() {
    PhysicalPlan plan = parse(new TestScan(), DSL.ref("ip", STRING),
        "^(?<ip>(\\d{1,3}\\.){2}).+$", ImmutableMap.of("ip", ""));
    List<ExprValue> results = execute(plan);
    assertEquals(5, results.size());
    results.forEach(result -> {
      assertEquals(2,
          result.tupleValue().get("ip").stringValue().chars().filter(ch -> ch == '.').count());
    });
  }

  @Test
  public void nonStringFieldShouldThrowSemanticException() {
    PhysicalPlan plan = parse(new TestScan(), DSL.ref("response", INTEGER),
        "(?<response>.*)", ImmutableMap.of("response", ""));
    assertThrows(SemanticCheckException.class, () -> execute(plan));
  }

  @Test
  public void wrongTypeCastingShouldThrowSemanticException() {
    PhysicalPlan plan = parse(new TestScan(), DSL.ref("ip", STRING),
        "(?<ipMatchedINTEGER>^(\\d{1,3}\\.){3}\\d{1,3}$)", ImmutableMap.of("ipMatched", "INTEGER"));
    assertThrows(SemanticCheckException.class, () -> execute(plan));
  }

  @Test
  public void noMatchShouldReturnNull() {
    PhysicalPlan plan = parse(new TestScan(), DSL.ref("ip", STRING),
        "(?<ipMatched>^$)", ImmutableMap.of("ipMatched", ""));
    List<ExprValue> results = execute(plan);
    assertEquals(5, results.size());
    results.forEach(result -> assertEquals(null, result.tupleValue().get("ipMatched")));
  }

  @Test
  public void nullValueShouldReturnNull() {
    LinkedHashMap<String, ExprValue> value = new LinkedHashMap<>();
    value.put("ip", LITERAL_NULL);
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(new ExprTupleValue(value));

    PhysicalPlan plan = parse(inputPlan, DSL.ref("ip", STRING),
        "(?<ipMatched>^(\\d{1,3}\\.){3}\\d{1,3}$)", ImmutableMap.of("ipMatched", ""));
    List<ExprValue> results = execute(plan);
    assertEquals(1, results.size());
    results.forEach(result -> assertEquals(null, result.tupleValue().get("ipMatched")));
  }

  @Test
  public void missingValueShouldReturnNull() {
    LinkedHashMap<String, ExprValue> value = new LinkedHashMap<>();
    value.put("ip", LITERAL_MISSING);
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(new ExprTupleValue(value));

    PhysicalPlan plan = parse(inputPlan, DSL.ref("ip", STRING),
        "(?<ipMatched>^(\\d{1,3}\\.){3}\\d{1,3}$)", ImmutableMap.of("ipMatched", ""));
    List<ExprValue> results = execute(plan);
    assertEquals(1, results.size());
    results.forEach(result -> assertEquals(null, result.tupleValue().get("ipMatched")));
  }

  @Test
  public void do_nothing_with_none_tuple_value() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(ExprValueUtils.integerValue(1));
    PhysicalPlan plan = parse(inputPlan, DSL.ref("ip", STRING),
        "(?<ipMatched>^(\\d{1,3}\\.){3}\\d{1,3}$)", ImmutableMap.of("ipMatched", ""));
    List<ExprValue> result = execute(plan);

    assertThat(result, allOf(iterableWithSize(1), hasItems(ExprValueUtils.integerValue(1))));
  }

}
