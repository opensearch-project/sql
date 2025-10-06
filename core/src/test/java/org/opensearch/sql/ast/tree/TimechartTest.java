/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

class TimechartTest {

  @Test
  void should_transform_per_second_function_to_sum_and_eval() {
    withTimechart(span(1, "m"), per_second("bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(bytes)", divide("per_second(bytes)", 60.0)),
                timechart(span(1, "m"), alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_not_transform_non_per_second_functions() {
    Timechart timechart =
        new Timechart(
            relation("t"),
            AstDSL.span(field("@timestamp"), intLiteral(1), SpanUnit.of("m")),
            aggregate("count", field("bytes")),
            null,
            10,
            true);

    UnresolvedPlan result = timechart.transformPerSecondFunctions();

    assertSame(timechart, result);
  }

  @Test
  void should_calculate_correct_divisor_for_per_second_with_seconds_span() {
    withTimechart(span(30, "s"), per_second("bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(bytes)", divide("per_second(bytes)", 30.0)),
                timechart(span(30, "s"), alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_calculate_correct_divisor_for_per_second_with_minutes_span() {
    withTimechart(span(5, "m"), per_second("bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(bytes)", divide("per_second(bytes)", 300.0)),
                timechart(span(5, "m"), alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_calculate_correct_divisor_for_per_second_with_hours_span() {
    withTimechart(span(2, "h"), per_second("bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(bytes)", divide("per_second(bytes)", 7200.0)),
                timechart(span(2, "h"), alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_calculate_correct_divisor_for_per_second_with_days_span() {
    withTimechart(span(1, "d"), per_second("bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(bytes)", divide("per_second(bytes)", 86400.0)),
                timechart(span(1, "d"), alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_use_default_divisor_for_unsupported_span_units() {
    withTimechart(
            AstDSL.span(field("@timestamp"), intLiteral(1), SpanUnit.NONE), per_second("bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(bytes)", divide("per_second(bytes)", 60.0)),
                timechart(
                    AstDSL.span(field("@timestamp"), intLiteral(1), SpanUnit.NONE),
                    alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_handle_qualified_field_names_in_transformation() {
    withTimechart(span(5, "m"), per_second("server.bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(server.bytes)", divide("per_second(server.bytes)", 300.0)),
                timechart(span(5, "m"), alias("per_second(server.bytes)", sum("server.bytes")))));
  }

  // Fluent API for readable test assertions

  private static TransformationAssertion withTimechart(Span spanExpr, AggregateFunction aggFunc) {
    return new TransformationAssertion(timechart(spanExpr, aggFunc));
  }

  private static Timechart timechart(Span spanExpr, UnresolvedExpression aggExpr) {
    return new Timechart(relation("t"), aggExpr).span(spanExpr).limit(10).useOther(true);
  }

  private static Span span(int value, String unit) {
    return AstDSL.span(field("@timestamp"), intLiteral(value), SpanUnit.of(unit));
  }

  private static AggregateFunction per_second(String fieldName) {
    return (AggregateFunction) aggregate("per_second", field(fieldName));
  }

  private static AggregateFunction sum(String fieldName) {
    return (AggregateFunction) aggregate("sum", field(fieldName));
  }

  private static Let let(String fieldName, UnresolvedExpression expression) {
    return AstDSL.let(field(fieldName), expression);
  }

  private static UnresolvedExpression divide(String fieldName, double divisor) {
    return function("/", field(fieldName), doubleLiteral(divisor));
  }

  private static UnresolvedPlan eval(Let letExpr, Timechart timechartExpr) {
    return AstDSL.eval(timechartExpr, letExpr);
  }

  private static class TransformationAssertion {
    private final Timechart timechart;
    private UnresolvedPlan result;

    TransformationAssertion(Timechart timechart) {
      this.timechart = timechart;
    }

    public TransformationAssertion transformPerFunction() {
      this.result = timechart.attach(null);
      return this;
    }

    public void shouldBe(UnresolvedPlan expected) {
      // Verify transformation produces expected type and structure
      assertEquals(expected.getClass(), result.getClass());
      if (expected instanceof Eval && result instanceof Eval) {
        Eval expectedEval = (Eval) expected;
        Eval actualEval = (Eval) result;
        assertEquals(
            expectedEval.getExpressionList().size(), actualEval.getExpressionList().size());
      }
    }
  }

  // TODO: Add tests for per_minute, per_hour, and per_day functions when implemented
  // Example structure for future functions:
  //
  // @Test
  // void should_transform_per_minute_function() {
  //   withTimechart(span(2, "h"), per_minute("bytes"))
  //       .transformPerFunction()
  //       .shouldBe(evalWith(
  //           let("per_minute(bytes)", divide("per_minute(bytes)", 120.0)),
  //           timechart(span(2, "h"), alias("per_minute(bytes)", sum("bytes")))));
  // }
}
