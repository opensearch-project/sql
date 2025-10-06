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

  /**
   * Transform per_second functions into sum + eval pattern. Simple approach for single aggregation
   * function only.
   */
  private UnresolvedPlan transformPerSecondFunctions() {
    AggregateFunction aggFunc = (AggregateFunction) this.aggregateFunction;
    if (!"per_second".equals(aggFunc.getFuncName())) {
      return this;
    }

    String fieldName = extractFieldName(aggFunc.getField());
    String originalName = "per_second(" + fieldName + ")";
    double divisor = (double) (extractIntervalSeconds() / 1); // unit = 1 for per_second
    return eval(
        timechart(AstDSL.alias(originalName, aggregate("sum", aggFunc.getField()))),
        let(
            AstDSL.field(originalName),
            function("/", AstDSL.field(originalName), doubleLiteral(divisor))));
  }

  private String extractFieldName(UnresolvedExpression field) {
    return field instanceof Field ? ((Field) field).getField().toString() : field.toString();
  }

  private Timechart timechart(UnresolvedExpression newAggregateFunction) {
    return this.toBuilder().aggregateFunction(newAggregateFunction).build();
  }

  private long extractIntervalSeconds() {
    Span span = (Span) this.binExpression;
    int intervalValue = ((Number) ((Literal) span.getValue()).getValue()).intValue();
    String unitString = SpanUnit.getName(span.getUnit());
    TimeUnitConfig config = TimeUnitRegistry.getConfig(unitString);
    return config != null ? config.toSeconds(intervalValue) : 60L;
  }
}
