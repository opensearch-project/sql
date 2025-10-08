/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node represent Timechart operation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@lombok.Builder(toBuilder = true)
public class Timechart extends UnresolvedPlan {
  private UnresolvedPlan child;
  private UnresolvedExpression binExpression;
  private UnresolvedExpression aggregateFunction;
  private UnresolvedExpression byField;
  private Integer limit;
  private Boolean useOther;

  public Timechart(UnresolvedPlan child, UnresolvedExpression aggregateFunction) {
    this(child, null, aggregateFunction, null, null, true);
  }

  public Timechart span(UnresolvedExpression binExpression) {
    return toBuilder().binExpression(binExpression).build();
  }

  public Timechart by(UnresolvedExpression byField) {
    return toBuilder().byField(byField).build();
  }

  public Timechart limit(Integer limit) {
    return toBuilder().limit(limit).build();
  }

  public Timechart useOther(Boolean useOther) {
    return toBuilder().useOther(useOther).build();
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return toBuilder().child(child).build().transformPerSecondFunctions();
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitTimechart(this, context);
  }

  /** Transform per_second functions into sum + eval pattern with runtime calculation. */
  private UnresolvedPlan transformPerSecondFunctions() {
    AggregateFunction aggFunc = (AggregateFunction) this.aggregateFunction;
    if (!"per_second".equals(aggFunc.getFuncName())) {
      return this;
    }

    String fieldName = extractFieldName(aggFunc.getField());
    String originalName = "per_second(" + fieldName + ")";
    Span span = (Span) this.binExpression;
    return eval(
        timechart(AstDSL.alias(originalName, sum(aggFunc.getField()))),
        let(
            originalName,
            divide(
                multiply(originalName, perUnitSeconds()),
                timestampdiff(
                    "SECOND",
                    "@timestamp", // bin start time
                    timestampadd(
                        getSpanUnitForTimestampAdd(span),
                        span.getValue(),
                        "@timestamp") // bin end time
                    ))));
  }

  private String extractFieldName(UnresolvedExpression field) {
    return field instanceof Field ? ((Field) field).getField().toString() : field.toString();
  }

  private Timechart timechart(UnresolvedExpression newAggregateFunction) {
    return this.toBuilder().aggregateFunction(newAggregateFunction).build();
  }

  private UnresolvedExpression perUnitSeconds() {
    return doubleLiteral(1.0);
  }

  /** Converts SpanUnit to the format expected by timestampadd function. */
  private String getSpanUnitForTimestampAdd(Span span) {
    String unitString = SpanUnit.getName(span.getUnit());
    // Map common units to timestampadd format
    switch (unitString.toLowerCase()) {
      case "s":
      case "sec":
      case "second":
      case "seconds":
        return "SECOND";
      case "m":
      case "min":
      case "minute":
      case "minutes":
        return "MINUTE";
      case "h":
      case "hour":
      case "hours":
        return "HOUR";
      case "d":
      case "day":
      case "days":
        return "DAY";
      case "w":
      case "week":
      case "weeks":
        return "WEEK";
      case "mon":
      case "month":
      case "months":
        return "MONTH";
      case "q":
      case "quarter":
      case "quarters":
        return "QUARTER";
      case "y":
      case "year":
      case "years":
        return "YEAR";
      default:
        return "MINUTE"; // default fallback
    }
  }

  // Private DSL methods for clean transformation code

  private UnresolvedExpression sum(UnresolvedExpression field) {
    return aggregate("sum", field);
  }

  private UnresolvedExpression multiply(String fieldName, UnresolvedExpression value) {
    return function("*", AstDSL.field(fieldName), value);
  }

  private UnresolvedExpression divide(UnresolvedExpression left, UnresolvedExpression right) {
    return function("/", left, right);
  }

  private UnresolvedExpression timestampadd(
      String unit, UnresolvedExpression value, String timestampField) {
    return function("timestampadd", stringLiteral(unit), value, AstDSL.field(timestampField));
  }

  private UnresolvedExpression timestampdiff(
      String unit, String startField, UnresolvedExpression end) {
    return function("timestampdiff", stringLiteral(unit), AstDSL.field(startField), end);
  }

  private Let let(String fieldName, UnresolvedExpression expression) {
    return AstDSL.let(AstDSL.field(fieldName), expression);
  }
}
