/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
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

  /**
   * Transform per_second functions into sum + eval pattern. Simple approach for single aggregation
   * function only.
   */
  public UnresolvedPlan transformPerSecondFunctions() {
    if (this.aggregateFunction == null || !(this.aggregateFunction instanceof AggregateFunction)) {
      return this;
    }

    AggregateFunction aggFunc = (AggregateFunction) this.aggregateFunction;
    if (!"per_second".equals(aggFunc.getFuncName())) {
      return this; // No per_second function found
    }

    // Extract interval seconds from binExpression
    long intervalSec = extractIntervalSeconds();

    // Per-second unit = 1 (later add per_minute=60, per_hour=3600, etc.)
    long unit = 1;

    // Get the field from per_second(field)
    UnresolvedExpression field = aggFunc.getField();

    // Create sum(field) to replace per_second(field)
    AggregateFunction sumFunc = new AggregateFunction("sum", field);

    // Create eval: per_second(field) = sum(field) / (intervalSec / unit)
    String fieldName =
        field instanceof Field ? ((Field) field).getField().toString() : field.toString();
    String originalName = "per_second(" + fieldName + ")";
    String sumName = "sum(" + fieldName + ")";

    Let evalTransform =
        new Let(
            new Field(AstDSL.qualifiedName(originalName)),
            new Function(
                "/",
                Arrays.asList(
                    new Field(AstDSL.qualifiedName(sumName)),
                    new Literal((double) (intervalSec / unit), DataType.DOUBLE))));

    this.aggregateFunction = sumFunc;

    // Create eval that wraps the new timechart
    Eval evalNode = new Eval(Arrays.asList(evalTransform));
    Eval newEval = evalNode.attach(this);
    return newEval;
  }

  private long extractIntervalSeconds() {
    // Simple extraction assuming span=literal format
    if (!(this.binExpression instanceof Span)) {
      return 60L; // Default 1 minute
    }

    Span span = (Span) this.binExpression;
    int intervalValue =
        ((Literal) span.getValue()).getValue() instanceof Number
            ? ((Number) ((Literal) span.getValue()).getValue()).intValue()
            : 1;

    String unitString = SpanUnit.getName(span.getUnit());

    // Use existing TimeUnitRegistry
    TimeUnitConfig config = TimeUnitRegistry.getConfig(unitString);
    return config != null ? config.toSeconds(intervalValue) : 60L;
  }
}
