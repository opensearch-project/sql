/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.let;

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
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.binning.time.TimeUnitConfig;
import org.opensearch.sql.calcite.utils.binning.time.TimeUnitRegistry;

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

    UnresolvedExpression intervalSecondsExpr = getIntervalSecondsExpr();
    UnresolvedExpression perUnitSecondsExpr = getPerUnitSecondsExpr(aggFunc.getFuncName());

    // PPL divide function handles type coercion automatically (long/double -> double result)
    UnresolvedExpression runtimeDivisor =
        function("*", function("/", AstDSL.field(originalName), intervalSecondsExpr), perUnitSecondsExpr);

    return eval(
        timechart(AstDSL.alias(originalName, aggregate("sum", aggFunc.getField()))),
        let(AstDSL.field(originalName), runtimeDivisor));
  }

  private String extractFieldName(UnresolvedExpression field) {
    return field instanceof Field ? ((Field) field).getField().toString() : field.toString();
  }

  private Timechart timechart(UnresolvedExpression newAggregateFunction) {
    return this.toBuilder().aggregateFunction(newAggregateFunction).build();
  }

  /**
   * Creates a runtime expression to calculate interval seconds using timestampdiff. Formula:
   * timestampdiff(SECOND, @timestamp, timestampadd(unit, value, @timestamp))
   */
  private UnresolvedExpression getIntervalSecondsExpr() {
    Span span = (Span) this.binExpression;
    UnresolvedExpression timestampField = AstDSL.field("@timestamp");
    UnresolvedExpression spanUnit = AstDSL.stringLiteral(getSpanUnitForTimestampAdd(span));
    UnresolvedExpression spanValue = span.getValue();

    // timestampadd(unit, value, @timestamp) - calculates bin end time
    UnresolvedExpression binEndTime = function("timestampadd", spanUnit, spanValue, timestampField);

    // timestampdiff(SECOND, @timestamp, bin_end_time) - calculates actual seconds in span
    return function("timestampdiff", AstDSL.stringLiteral("SECOND"), timestampField, binEndTime);
  }

  /**
   * Returns the per-unit seconds as an expression for the given function. per_second: 1.0,
   * per_minute: 60.0, per_hour: 3600.0 (using double for floating-point arithmetic)
   */
  private UnresolvedExpression getPerUnitSecondsExpr(String funcName) {
    switch (funcName) {
      case "per_second":
        return doubleLiteral(1.0);
      case "per_minute":
        return doubleLiteral(60.0);
      case "per_hour":
        return doubleLiteral(3600.0);
      default:
        return doubleLiteral(1.0); // default to per_second
    }
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

  private long extractIntervalSeconds() {
    Span span = (Span) this.binExpression;
    int intervalValue = ((Number) ((Literal) span.getValue()).getValue()).intValue();
    String unitString = SpanUnit.getName(span.getUnit());
    TimeUnitConfig config = TimeUnitRegistry.getConfig(unitString);
    return config.toSeconds(intervalValue);
  }
}
