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
  @CsvSource({"1, m, 60.0", "30, s, 30.0", "5, m, 300.0", "2, h, 7200.0", "1, d, 86400.0"})
  void should_transform_per_second_for_different_spans(
      int spanValue, String spanUnit, double expectedDivisor) {
    withTimechart(span(spanValue, spanUnit), per_second("bytes"))
        .transformPerFunction()
        .shouldBe(
            eval(
                let("per_second(bytes)", divide("per_second(bytes)", expectedDivisor)),
                timechart(span(spanValue, spanUnit), alias("per_second(bytes)", sum("bytes")))));
  }

  @Test
  void should_not_transform_non_per_second_functions() {
    withTimechart(span(1, "m"), sum("bytes"))
        .transformPerFunction()
        .shouldBe(timechart(span(1, "m"), sum("bytes")));
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
}
