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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

class TimechartTest {

  @ParameterizedTest
  @CsvSource({"1, m, MINUTE", "30, s, SECOND", "5, m, MINUTE", "2, h, HOUR", "1, d, DAY"})
  void should_transform_per_second_for_different_spans(
      int spanValue, String spanUnit, String expectedIntervalUnit) {
    withTimechart(span(spanValue, spanUnit), perSecond("bytes"))
        .whenTransformingPerFunction()
        .thenExpect(
            eval(
                let(
                    "per_second(bytes)",
                    divide(
                        multiply("per_second(bytes)", 1.0),
                        timestampdiff(
                            "SECOND",
                            "@timestamp",
                            timestampadd(expectedIntervalUnit, spanValue, "@timestamp")))),
                timechart(span(spanValue, spanUnit), alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_not_transform_non_per_functions() {
    withTimechart(span(1, "m"), sum("bytes"))
        .whenTransformingPerFunction()
        .thenExpect(timechart(span(1, "m"), sum("bytes")));
  }

  @Test
  void should_preserve_all_fields_during_per_function_transformation() {
    Timechart original =
        new Timechart(relation("logs"), perSecond("bytes"))
            .span(span(5, "m"))
            .by(field("status"))
            .limit(20)
            .useOther(false);

    Timechart expected =
        new Timechart(relation("logs"), alias("per_second(bytes)", sum("bytes")))
            .span(span(5, "m"))
            .by(field("status"))
            .limit(20)
            .useOther(false);

    withTimechart(original)
        .whenTransformingPerFunction()
        .thenExpect(
            eval(
                let(
                    "per_second(bytes)",
                    divide(
                        multiply("per_second(bytes)", 1.0),
                        timestampdiff(
                            "SECOND", "@timestamp", timestampadd("MINUTE", 5, "@timestamp")))),
                expected));
  }

  // Fluent API for readable test assertions

  private static TransformationAssertion withTimechart(Span spanExpr, AggregateFunction aggFunc) {
    return new TransformationAssertion(timechart(spanExpr, aggFunc));
  }

  private static TransformationAssertion withTimechart(Timechart timechart) {
    return new TransformationAssertion(timechart);
  }

  private static Timechart timechart(Span spanExpr, UnresolvedExpression aggExpr) {
    // Set child here because expected object won't call attach below
    return new Timechart(relation("t"), aggExpr).span(spanExpr).limit(10).useOther(true);
  }

  private static Span span(int value, String unit) {
    return AstDSL.span(field("@timestamp"), intLiteral(value), SpanUnit.of(unit));
  }

  private static AggregateFunction perSecond(String fieldName) {
    return (AggregateFunction) aggregate("per_second", field(fieldName));
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
        "timestampadd", AstDSL.stringLiteral(unit), intLiteral(value), field(timestampField));
  }

  private static UnresolvedExpression timestampdiff(
      String unit, String startField, UnresolvedExpression end) {
    return function("timestampdiff", AstDSL.stringLiteral(unit), field(startField), end);
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

    public TransformationAssertion whenTransformingPerFunction() {
      this.result = timechart.attach(timechart.getChild().get(0));
      return this;
    }

    public void thenExpect(UnresolvedPlan expected) {
      assertEquals(expected, result);
    }
  }
}
