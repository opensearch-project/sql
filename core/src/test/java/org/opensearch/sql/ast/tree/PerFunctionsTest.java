/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

class PerFunctionsTest {
  /**
   * @return test sources for per_* function test.
   */
  private static Stream<Arguments> perFuncTestSources() {
    return Stream.of(
        Arguments.of(30, "s", "SECOND"),
        Arguments.of(5, "m", "MINUTE"),
        Arguments.of(2, "h", "HOUR"),
        Arguments.of(1, "d", "DAY"),
        Arguments.of(1, "w", "WEEK"),
        Arguments.of(1, "M", "MONTH"),
        Arguments.of(1, "q", "QUARTER"),
        Arguments.of(1, "y", "YEAR"));
  }

  @ParameterizedTest
  @MethodSource("perFuncTestSources")
  void should_transform_per_second_for_different_spans(
      int spanValue, String spanUnit, String expectedIntervalUnit) {
    withTimechart(span(spanValue, spanUnit), perSecond("bytes"))
        .whenTransformingPerFunction()
        .thenExpect(
            eval(
                let(
                    "per_second(bytes)",
                    divide(
                        multiply("per_second(bytes)", 1000.0),
                        timestampdiff(
                            "MILLISECOND",
                            IMPLICIT_FIELD_TIMESTAMP,
                            timestampadd(
                                expectedIntervalUnit, spanValue, IMPLICIT_FIELD_TIMESTAMP)))),
                timechart(span(spanValue, spanUnit), alias("per_second(bytes)", sum("bytes")))));
  }

  @ParameterizedTest
  @MethodSource("perFuncTestSources")
  void should_transform_per_minute_for_different_spans(
      int spanValue, String spanUnit, String expectedIntervalUnit) {
    withTimechart(span(spanValue, spanUnit), perMinute("bytes"))
        .whenTransformingPerFunction()
        .thenExpect(
            eval(
                let(
                    "per_minute(bytes)",
                    divide(
                        multiply("per_minute(bytes)", 60000.0),
                        timestampdiff(
                            "MILLISECOND",
                            IMPLICIT_FIELD_TIMESTAMP,
                            timestampadd(
                                expectedIntervalUnit, spanValue, IMPLICIT_FIELD_TIMESTAMP)))),
                timechart(span(spanValue, spanUnit), alias("per_minute(bytes)", sum("bytes")))));
  }

  @ParameterizedTest
  @MethodSource("perFuncTestSources")
  void should_transform_per_hour_for_different_spans(
      int spanValue, String spanUnit, String expectedIntervalUnit) {
    withTimechart(span(spanValue, spanUnit), perHour("bytes"))
        .whenTransformingPerFunction()
        .thenExpect(
            eval(
                let(
                    "per_hour(bytes)",
                    divide(
                        multiply("per_hour(bytes)", 3600000.0),
                        timestampdiff(
                            "MILLISECOND",
                            IMPLICIT_FIELD_TIMESTAMP,
                            timestampadd(
                                expectedIntervalUnit, spanValue, IMPLICIT_FIELD_TIMESTAMP)))),
                timechart(span(spanValue, spanUnit), alias("per_hour(bytes)", sum("bytes")))));
  }

  @ParameterizedTest
  @MethodSource("perFuncTestSources")
  void should_transform_per_day_for_different_spans(
      int spanValue, String spanUnit, String expectedIntervalUnit) {
    withTimechart(span(spanValue, spanUnit), perDay("bytes"))
        .whenTransformingPerFunction()
        .thenExpect(
            eval(
                let(
                    "per_day(bytes)",
                    divide(
                        multiply("per_day(bytes)", 8.64E7),
                        timestampdiff(
                            "MILLISECOND",
                            IMPLICIT_FIELD_TIMESTAMP,
                            timestampadd(
                                expectedIntervalUnit, spanValue, IMPLICIT_FIELD_TIMESTAMP)))),
                timechart(span(spanValue, spanUnit), alias("per_day(bytes)", sum("bytes")))));
  }

  @Test
  void should_not_transform_non_per_functions() {
    withTimechart(span(1, "m"), sum("bytes"))
        .whenTransformingPerFunction()
        .thenExpect(timechart(span(1, "m"), sum("bytes")));
  }

  @Test
  void should_preserve_all_fields_during_per_function_transformation() {
    Chart original =
        Chart.builder()
            .child(relation("logs"))
            .aggregationFunction(perSecond("bytes"))
            .rowSplit(span(5, "m"))
            .columnSplit(field("status"))
            .arguments(
                List.of(
                    new Argument("limit", intLiteral(20)), new Argument("useOther", Literal.FALSE)))
            .build();

    Chart expected =
        Chart.builder()
            .child(relation("logs"))
            .aggregationFunction(alias("per_second(bytes)", sum("bytes")))
            .rowSplit(span(5, "m"))
            .columnSplit(field("status"))
            .arguments(
                List.of(
                    new Argument("limit", intLiteral(20)), new Argument("useOther", Literal.FALSE)))
            .build();

    withTimechart(original)
        .whenTransformingPerFunction()
        .thenExpect(
            eval(
                let(
                    "per_second(bytes)",
                    divide(
                        multiply("per_second(bytes)", 1000.0),
                        timestampdiff(
                            "MILLISECOND",
                            IMPLICIT_FIELD_TIMESTAMP,
                            timestampadd("MINUTE", 5, IMPLICIT_FIELD_TIMESTAMP)))),
                expected));
  }

  // Fluent API for readable test assertions

  private static TransformationAssertion withTimechart(Span spanExpr, AggregateFunction aggFunc) {
    return new TransformationAssertion(timechart(spanExpr, aggFunc));
  }

  private static TransformationAssertion withTimechart(Chart timechart) {
    return new TransformationAssertion(timechart);
  }

  private static Chart timechart(Span spanExpr, UnresolvedExpression aggExpr) {
    // Set child here because expected object won't call attach below
    return Chart.builder()
        .child(relation("t"))
        .aggregationFunction(aggExpr)
        .rowSplit(spanExpr)
        .build();
  }

  private static Span span(int value, String unit) {
    return AstDSL.span(AstDSL.implicitTimestampField(), intLiteral(value), SpanUnit.of(unit));
  }

  private static AggregateFunction perSecond(String fieldName) {
    return (AggregateFunction) aggregate("per_second", field(fieldName));
  }

  private static AggregateFunction perMinute(String fieldName) {
    return (AggregateFunction) aggregate("per_minute", field(fieldName));
  }

  private static AggregateFunction perHour(String fieldName) {
    return (AggregateFunction) aggregate("per_hour", field(fieldName));
  }

  private static AggregateFunction perDay(String fieldName) {
    return (AggregateFunction) aggregate("per_day", field(fieldName));
  }

  private static AggregateFunction sum(String fieldName) {
    return (AggregateFunction) aggregate("sum", field(fieldName));
  }

  private static Let let(String fieldName, UnresolvedExpression expression) {
    return AstDSL.let(field(fieldName), expression);
  }

  private static UnresolvedExpression multiply(String fieldName, double right) {
    return function("*", field(fieldName), doubleLiteral(right));
  }

  private static UnresolvedExpression divide(
      UnresolvedExpression left, UnresolvedExpression right) {
    return function("/", left, right);
  }

  private static UnresolvedExpression timestampadd(String unit, int value, String timestampField) {
    return function(
        BuiltinFunctionName.TIMESTAMPADD.getName().getFunctionName(),
        AstDSL.stringLiteral(unit),
        intLiteral(value),
        field(timestampField));
  }

  private static UnresolvedExpression timestampdiff(
      String unit, String startField, UnresolvedExpression end) {

    return function(
        BuiltinFunctionName.TIMESTAMPDIFF.getName().getFunctionName(),
        AstDSL.stringLiteral(unit),
        field(startField),
        end);
  }

  private static UnresolvedPlan eval(Let letExpr, Chart timechartExpr) {
    return AstDSL.eval(timechartExpr, letExpr);
  }

  private static class TransformationAssertion {
    private final Chart timechart;
    private UnresolvedPlan result;

    TransformationAssertion(Chart timechart) {
      this.timechart = timechart;
    }

    public TransformationAssertion whenTransformingPerFunction() {
      this.result = timechart.attach(timechart.getChild().get(0));
      return this;
    }

    public void thenExpect(UnresolvedPlan expected) {
      assertEquals(expected, result);
    }
  }
}
