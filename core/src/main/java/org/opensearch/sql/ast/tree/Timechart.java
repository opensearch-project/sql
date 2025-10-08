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
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.PlanUtils;

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

    Span span = (Span) this.binExpression;
    String aggName = aggregationName(aggFunc);
    Field aggField = AstDSL.field(aggName);
    Field spanStartTime = AstDSL.field("@timestamp");
    Function spanEndTime = timestampadd(span.getUnit(), span.getValue(), spanStartTime);
    return eval(
        timechart(AstDSL.alias(aggName, aggregate("sum", aggFunc.getField()))),
        let(
            aggField,
            function(
                "/",
                function("*", aggField, perUnitSeconds()),
                timestampdiff("SECOND", spanStartTime, spanEndTime))));
  }

  private String aggregationName(AggregateFunction aggFunc) {
    UnresolvedExpression field = aggFunc.getField();
    String fieldName =
        field instanceof Field ? ((Field) field).getField().toString() : field.toString();
    return String.format(Locale.ROOT, "%s(%s)", aggFunc.getFuncName(), fieldName);
  }

  private Timechart timechart(UnresolvedExpression newAggregateFunction) {
    return this.toBuilder().aggregateFunction(newAggregateFunction).build();
  }

  private UnresolvedExpression perUnitSeconds() {
    return doubleLiteral(1.0);
  }

  private Function timestampadd(
      SpanUnit unit, UnresolvedExpression value, UnresolvedExpression timestampField) {
    UnresolvedExpression intervalUnit =
        stringLiteral(PlanUtils.spanUnitToIntervalUnit(unit).toString());
    return function("timestampadd", intervalUnit, value, timestampField);
  }

  private Function timestampdiff(
      String unit, UnresolvedExpression start, UnresolvedExpression end) {
    return function("timestampdiff", stringLiteral(unit), start, end);
  }
}
