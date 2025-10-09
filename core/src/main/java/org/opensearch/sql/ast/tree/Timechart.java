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
import static org.opensearch.sql.ast.expression.IntervalUnit.SECOND;
import static org.opensearch.sql.ast.tree.Timechart.PerFunctionRateExprBuilder.sum;
import static org.opensearch.sql.ast.tree.Timechart.PerFunctionRateExprBuilder.timestampadd;
import static org.opensearch.sql.ast.tree.Timechart.PerFunctionRateExprBuilder.timestampdiff;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DIVIDE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTIPLY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPDIFF;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.Let;
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
    // Transform after child attached to avoid unintentionally overriding it
    return toBuilder().child(child).build().transformPerSecondFunction();
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
   * Transform per function to eval-based post-processing on sum result by timechart. Specifically,
   * calculate how many seconds are in the time bucket based on the span option dynamically, then
   * divide the aggregated sum value by the number of seconds to get the per-second rate.
   *
   * <p>For example, with span=5m per_second(field): per second rate = sum(field) / 300 seconds
   *
   * @return eval+timechart if per function present, or the original timechart otherwise.
   */
  private UnresolvedPlan transformPerSecondFunction() {
    Optional<PerFunction> perFuncOpt = PerFunction.from(aggregateFunction);
    if (perFuncOpt.isEmpty()) {
      return this;
    }

    PerFunction perFunc = perFuncOpt.get();
    Span span = (Span) this.binExpression;
    Field spanStartTime = AstDSL.field(IMPLICIT_FIELD_TIMESTAMP);
    Function spanEndTime = timestampadd(span.getUnit(), span.getValue(), spanStartTime);
    Function spanSeconds = timestampdiff(SECOND, spanStartTime, spanEndTime);

    return eval(
        timechart(AstDSL.alias(perFunc.aggName, sum(perFunc.aggArg))),
        let(perFunc.aggName).multiply(perFunc.seconds).dividedBy(spanSeconds));
  }

  private Timechart timechart(UnresolvedExpression newAggregateFunction) {
    return this.toBuilder().aggregateFunction(newAggregateFunction).build();
  }

  /** TODO: extend to support additional per_* functions */
  @RequiredArgsConstructor
  private static final class PerFunction {
    private static final Map<String, Integer> UNIT_SECONDS = Map.of("per_second", 1);
    private final String aggName;
    private final UnresolvedExpression aggArg;
    private final int seconds;

    static Optional<PerFunction> from(UnresolvedExpression aggFunc) {
      if (!(aggFunc instanceof AggregateFunction agg)) {
        return Optional.empty();
      }

      String aggFuncName = agg.getFuncName().toLowerCase(Locale.ROOT);
      if (!UNIT_SECONDS.containsKey(aggFuncName)) {
        return Optional.empty();
      }

      String fieldName =
          (agg.getField() instanceof Field)
              ? ((Field) agg.getField()).getField().toString()
              : agg.getField().toString();
      String aggName = String.format(Locale.ROOT, "%s(%s)", aggFuncName, fieldName);
      return Optional.of(new PerFunction(aggName, agg.getField(), UNIT_SECONDS.get(aggFuncName)));
    }
  }

  private PerFunctionRateExprBuilder let(String fieldName) {
    return new PerFunctionRateExprBuilder(AstDSL.field(fieldName));
  }

  /** Fluent builder for creating Let expressions with mathematical operations. */
  static class PerFunctionRateExprBuilder {
    private final Field field;
    private UnresolvedExpression expr;

    PerFunctionRateExprBuilder(Field field) {
      this.field = field;
      this.expr = field;
    }

    PerFunctionRateExprBuilder multiply(Integer multiplier) {
      // Promote to double literal to avoid integer division in downstream
      this.expr =
          function(
              MULTIPLY.getName().getFunctionName(), expr, doubleLiteral(multiplier.doubleValue()));
      return this;
    }

    Let dividedBy(UnresolvedExpression divisor) {
      return AstDSL.let(field, function(DIVIDE.getName().getFunctionName(), expr, divisor));
    }

    static UnresolvedExpression sum(UnresolvedExpression field) {
      return aggregate(SUM.getName().getFunctionName(), field);
    }

    static Function timestampadd(
        SpanUnit unit, UnresolvedExpression value, UnresolvedExpression timestampField) {
      UnresolvedExpression intervalUnit =
          stringLiteral(PlanUtils.spanUnitToIntervalUnit(unit).toString());
      return function(
          TIMESTAMPADD.getName().getFunctionName(), intervalUnit, value, timestampField);
    }

    static Function timestampdiff(
        IntervalUnit unit, UnresolvedExpression start, UnresolvedExpression end) {
      return function(
          TIMESTAMPDIFF.getName().getFunctionName(), stringLiteral(unit.toString()), start, end);
    }
  }
}
