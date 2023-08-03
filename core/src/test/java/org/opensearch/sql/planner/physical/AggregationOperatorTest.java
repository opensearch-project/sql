/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;

class AggregationOperatorTest extends PhysicalPlanTestBase {

  @Test
  public void sum_without_groups() {
    PhysicalPlan plan =
        new AggregationOperator(
            new TestScan(),
            Collections.singletonList(
                DSL.named("sum(response)", DSL.sum(DSL.ref("response", INTEGER)))),
            Collections.emptyList());
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(
        result,
        containsInAnyOrder(ExprValueUtils.tupleValue(ImmutableMap.of("sum(response)", 1504d))));
  }

  @Test
  public void avg_with_one_groups() {
    PhysicalPlan plan =
        new AggregationOperator(
            new TestScan(),
            Collections.singletonList(
                DSL.named("avg(response)", DSL.avg(DSL.ref("response", INTEGER)))),
            Collections.singletonList(DSL.named("action", DSL.ref("action", STRING))));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "avg(response)", 268d)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "avg(response)", 350d))));
  }

  @Test
  public void avg_with_two_groups() {
    PhysicalPlan plan =
        new AggregationOperator(
            new TestScan(),
            Collections.singletonList(
                DSL.named("avg(response)", DSL.avg(DSL.ref("response", INTEGER)))),
            Arrays.asList(
                DSL.named("action", DSL.ref("action", STRING)),
                DSL.named("ip", DSL.ref("ip", STRING))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("action", "GET", "ip", "209.160.24.63", "avg(response)", 302d)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("action", "GET", "ip", "112.111.162.4", "avg(response)", 200d)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("action", "POST", "ip", "74.125.19.106", "avg(response)", 350d))));
  }

  @Test
  public void sum_with_one_groups() {
    PhysicalPlan plan =
        new AggregationOperator(
            new TestScan(),
            Collections.singletonList(
                DSL.named("sum(response)", DSL.sum(DSL.ref("response", INTEGER)))),
            Collections.singletonList(DSL.named("action", DSL.ref("action", STRING))));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "sum(response)", 804)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "sum(response)", 700))));
  }

  @Test
  public void millisecond_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(datetimeInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("second", TIMESTAMP)))),
            Collections.singletonList(
                DSL.named(
                    "span", DSL.span(DSL.ref("second", TIMESTAMP), DSL.literal(6 * 1000), "ms"))));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("2021-01-01 00:00:00"), "count", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprTimestampValue("2021-01-01 00:00:12"), "count", 3))));
  }

  @Test
  public void second_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(datetimeInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("second", TIMESTAMP)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("second", TIMESTAMP), DSL.literal(6), "s"))));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("2021-01-01 00:00:00"), "count", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprTimestampValue("2021-01-01 00:00:12"), "count", 3))));
  }

  @Test
  public void minute_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(datetimeInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("minute", DATETIME)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("minute", DATETIME), DSL.literal(5), "m"))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDatetimeValue("2020-12-31 23:50:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDatetimeValue("2021-01-01 00:00:00"), "count", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprDatetimeValue("2021-01-01 00:05:00"), "count", 1))));

    plan =
        new AggregationOperator(
            testScan(datetimeInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("hour", TIME)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("hour", TIME), DSL.literal(30), "m"))));
    result = execute(plan);
    assertEquals(4, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimeValue("17:00:00"), "count", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimeValue("18:00:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimeValue("18:30:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimeValue("19:00:00"), "count", 1))));
  }

  @Test
  public void hour_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(datetimeInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("hour", TIME)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("hour", TIME), DSL.literal(1), "h"))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimeValue("17:00:00"), "count", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimeValue("18:00:00"), "count", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimeValue("19:00:00"), "count", 1))));
  }

  @Test
  public void day_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count(day)", DSL.count(DSL.ref("day", DATE)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("day", DATE), DSL.literal(1), "d"))));
    List<ExprValue> result = execute(plan);
    assertEquals(4, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-01-01"), "count(day)", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-01-02"), "count(day)", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-01-03"), "count(day)", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-01-04"), "count(day)", 1))));

    plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("month", DATE)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("month", DATE), DSL.literal(30), "d"))));
    result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2020-12-04"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-02-02"), "count", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-03-04"), "count", 1))));
  }

  @Test
  public void week_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("month", DATE)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("month", DATE), DSL.literal(5), "w"))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2020-11-16"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-01-25"), "count", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-03-01"), "count", 1))));
  }

  @Test
  public void month_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("month", DATE)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("month", DATE), DSL.literal(1), "M"))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2020-12-01"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-02-01"), "count", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDateValue("2021-03-01"), "count", 1))));

    plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("quarter", DATETIME)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("quarter", DATETIME), DSL.literal(2), "M"))));
    result = execute(plan);
    assertEquals(4, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDatetimeValue("2020-09-01 00:00:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDatetimeValue("2020-11-01 00:00:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDatetimeValue("2021-01-01 00:00:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprDatetimeValue("2021-05-01 00:00:00"), "count", 2))));

    plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("year", TIMESTAMP)))),
            Collections.singletonList(
                DSL.named(
                    "span", DSL.span(DSL.ref("year", TIMESTAMP), DSL.literal(10 * 12), "M"))));
    result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("1990-01-01 00:00:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("2010-01-01 00:00:00"), "count", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprTimestampValue("2020-01-01 00:00:00"), "count", 1))));
  }

  @Test
  public void quarter_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("quarter", DATETIME)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("quarter", DATETIME), DSL.literal(2), "q"))));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprDatetimeValue("2020-07-01 00:00:00"), "count", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprDatetimeValue("2021-01-01 00:00:00"), "count", 3))));

    plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("year", TIMESTAMP)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("year", TIMESTAMP), DSL.literal(10 * 4), "q"))));
    result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("1990-01-01 00:00:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("2010-01-01 00:00:00"), "count", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprTimestampValue("2020-01-01 00:00:00"), "count", 1))));
  }

  @Test
  public void year_span() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(dateInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("year", TIMESTAMP)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("year", TIMESTAMP), DSL.literal(10), "y"))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("1990-01-01 00:00:00"), "count", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("span", new ExprTimestampValue("2010-01-01 00:00:00"), "count", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprTimestampValue("2020-01-01 00:00:00"), "count", 1))));
  }

  @Test
  public void integer_field() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("integer", INTEGER)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("integer", INTEGER), DSL.literal(1), ""))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 2, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 5, "count", 1))));

    plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("integer", INTEGER)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("integer", INTEGER), DSL.literal(1.5), ""))));
    result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 0D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1.5D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 4.5D, "count", 1))));
  }

  @Test
  public void long_field() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("long", LONG)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("long", LONG), DSL.literal(1), ""))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1L, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 2L, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 5L, "count", 1))));

    plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("long", LONG)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("long", LONG), DSL.literal(1.5), ""))));
    result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 0D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1.5D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 4.5D, "count", 1))));
  }

  @Test
  public void float_field() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("float", FLOAT)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("float", FLOAT), DSL.literal(1), ""))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1F, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 2F, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 5F, "count", 1))));

    plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("float", FLOAT)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("float", FLOAT), DSL.literal(1.5), ""))));
    result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 0D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1.5D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 4.5D, "count", 1))));
  }

  @Test
  public void double_field() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("double", DOUBLE)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("double", DOUBLE), DSL.literal(1), ""))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 2D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 5D, "count", 1))));

    plan =
        new AggregationOperator(
            testScan(numericInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("double", DOUBLE)))),
            Collections.singletonList(
                DSL.named("span", DSL.span(DSL.ref("double", DOUBLE), DSL.literal(1.5), ""))));
    result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 0D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 1.5D, "count", 1)),
            ExprValueUtils.tupleValue(ImmutableMap.of("span", 4.5D, "count", 1))));
  }

  @Test
  public void twoBucketsSpanAndLong() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(compoundInputs),
            Collections.singletonList(DSL.named("max", DSL.max(DSL.ref("errors", INTEGER)))),
            Arrays.asList(
                DSL.named("span", DSL.span(DSL.ref("day", DATE), DSL.literal(1), "d")),
                DSL.named("region", DSL.ref("region", STRING))));
    List<ExprValue> result = execute(plan);
    assertEquals(4, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprDateValue("2021-01-03"), "region", "iad", "max", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprDateValue("2021-01-04"), "region", "iad", "max", 10)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprDateValue("2021-01-06"), "region", "iad", "max", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span", new ExprDateValue("2021-01-07"), "region", "iad", "max", 8))));

    plan =
        new AggregationOperator(
            testScan(compoundInputs),
            Collections.singletonList(DSL.named("max", DSL.max(DSL.ref("errors", INTEGER)))),
            Arrays.asList(
                DSL.named("span", DSL.span(DSL.ref("day", DATE), DSL.literal(1), "d")),
                DSL.named("region", DSL.ref("region", STRING)),
                DSL.named("host", DSL.ref("host", STRING))));
    result = execute(plan);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span",
                    new ExprDateValue("2021-01-03"),
                    "region",
                    "iad",
                    "host",
                    "h1",
                    "max",
                    2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span",
                    new ExprDateValue("2021-01-03"),
                    "region",
                    "iad",
                    "host",
                    "h2",
                    "max",
                    3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span",
                    new ExprDateValue("2021-01-04"),
                    "region",
                    "iad",
                    "host",
                    "h1",
                    "max",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span",
                    new ExprDateValue("2021-01-04"),
                    "region",
                    "iad",
                    "host",
                    "h2",
                    "max",
                    10)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span",
                    new ExprDateValue("2021-01-06"),
                    "region",
                    "iad",
                    "host",
                    "h1",
                    "max",
                    1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span",
                    new ExprDateValue("2021-01-07"),
                    "region",
                    "iad",
                    "host",
                    "h1",
                    "max",
                    6)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "span",
                    new ExprDateValue("2021-01-07"),
                    "region",
                    "iad",
                    "host",
                    "h2",
                    "max",
                    8))));
  }

  @Test
  public void aggregate_with_two_groups_with_windowing() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(compoundInputs),
            Collections.singletonList(DSL.named("sum", DSL.sum(DSL.ref("errors", INTEGER)))),
            Arrays.asList(
                DSL.named("host", DSL.ref("host", STRING)),
                DSL.named("span", DSL.span(DSL.ref("day", DATE), DSL.literal(1), "d"))));

    List<ExprValue> result = execute(plan);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-03"),
                    "sum", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-04"),
                    "sum", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-06"),
                    "sum", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-07"),
                    "sum", 6)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h2"),
                    "span", new ExprDateValue("2021-01-03"),
                    "sum", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h2"),
                    "span", new ExprDateValue("2021-01-04"),
                    "sum", 10)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h2"),
                    "span", new ExprDateValue("2021-01-07"),
                    "sum", 8))));
  }

  @Test
  public void aggregate_with_three_groups_with_windowing() {
    PhysicalPlan plan =
        new AggregationOperator(
            testScan(compoundInputs),
            Collections.singletonList(DSL.named("sum", DSL.sum(DSL.ref("errors", INTEGER)))),
            Arrays.asList(
                DSL.named("host", DSL.ref("host", STRING)),
                DSL.named("span", DSL.span(DSL.ref("day", DATE), DSL.literal(1), "d")),
                DSL.named("region", DSL.ref("region", STRING))));

    List<ExprValue> result = execute(plan);
    assertEquals(7, result.size());
    assertThat(
        result,
        containsInRelativeOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-03"),
                    "region", new ExprStringValue("iad"),
                    "sum", 2)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-04"),
                    "region", new ExprStringValue("iad"),
                    "sum", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-06"),
                    "region", new ExprStringValue("iad"),
                    "sum", 1)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h1"),
                    "span", new ExprDateValue("2021-01-07"),
                    "region", new ExprStringValue("iad"),
                    "sum", 6)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h2"),
                    "span", new ExprDateValue("2021-01-03"),
                    "region", new ExprStringValue("iad"),
                    "sum", 3)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h2"),
                    "span", new ExprDateValue("2021-01-04"),
                    "region", new ExprStringValue("iad"),
                    "sum", 10)),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "host", new ExprStringValue("h2"),
                    "span", new ExprDateValue("2021-01-07"),
                    "region", new ExprStringValue("iad"),
                    "sum", 8))));
  }

  @Test
  public void copyOfAggregationOperatorShouldSame() {
    AggregationOperator plan =
        new AggregationOperator(
            testScan(datetimeInputs),
            Collections.singletonList(DSL.named("count", DSL.count(DSL.ref("second", TIMESTAMP)))),
            Collections.singletonList(
                DSL.named(
                    "span", DSL.span(DSL.ref("second", TIMESTAMP), DSL.literal(6 * 1000), "ms"))));
    AggregationOperator copy =
        new AggregationOperator(
            plan.getInput(), plan.getAggregatorList(), plan.getGroupByExprList());

    assertEquals(plan, copy);
  }
}
