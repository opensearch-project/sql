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
import static org.opensearch.sql.ast.expression.IntervalUnit.MILLISECOND;
import static org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DIVIDE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTIPLY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPDIFF;
import static org.opensearch.sql.ast.tree.Chart.PerFunctionRateExprBuilder.timestampadd;
import static org.opensearch.sql.ast.tree.Chart.PerFunctionRateExprBuilder.timestampdiff;

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
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.PlanUtils;

/** AST node represent chart command. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@lombok.Builder(toBuilder = true)
public class Chart extends UnresolvedPlan {
  public static final Literal DEFAULT_USE_OTHER = Literal.TRUE;
  public static final Literal DEFAULT_OTHER_STR = AstDSL.stringLiteral("OTHER");
  public static final Literal DEFAULT_LIMIT = AstDSL.intLiteral(10);
  public static final Literal DEFAULT_USE_NULL = Literal.TRUE;
  public static final Literal DEFAULT_NULL_STR = AstDSL.stringLiteral("NULL");
  public static final Literal DEFAULT_TOP = Literal.TRUE;

  private UnresolvedPlan child;
  private UnresolvedExpression rowSplit;
  private UnresolvedExpression columnSplit;
  private UnresolvedExpression aggregationFunction;
  private List<Argument> arguments;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    // Transform after child attached to avoid unintentionally overriding it
    return toBuilder().child(child).build().transformPerFunction();
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChart(this, context);
  }

  /**
   * Transform per function to eval-based post-processing on sum result by chart. Specifically,
   * calculate how many seconds are in the time bucket based on the span option dynamically, then
   * divide the aggregated sum value by the number of seconds to get the per-second rate.
   *
   * <p>For example, with span=5m per_second(field): per second rate = sum(field) / 300 seconds
   *
   * @return eval+chart if per function present, or the original chart otherwise.
   */
  private UnresolvedPlan transformPerFunction() {
    Optional<PerFunction> perFuncOpt = PerFunction.from(aggregationFunction);
    if (perFuncOpt.isEmpty()) {
      return this;
    }

    PerFunction perFunc = perFuncOpt.get();
    // For chart, the rowSplit should contain the span information
    UnresolvedExpression spanExpr = rowSplit;
    if (rowSplit instanceof Alias) {
      spanExpr = ((Alias) rowSplit).getDelegated();
    }
    if (!(spanExpr instanceof Span)) {
      return this; // Cannot transform without span information
    }

    Span span = (Span) spanExpr;
    Field spanStartTime = AstDSL.field(IMPLICIT_FIELD_TIMESTAMP);
    Function spanEndTime = timestampadd(span.getUnit(), span.getValue(), spanStartTime);
    Function spanMillis = timestampdiff(MILLISECOND, spanStartTime, spanEndTime);
    final int SECOND_IN_MILLISECOND = 1000;
    return eval(
        chart(AstDSL.alias(perFunc.aggName, PerFunctionRateExprBuilder.sum(perFunc.aggArg))),
        let(perFunc.aggName)
            .multiply(perFunc.seconds * SECOND_IN_MILLISECOND)
            .dividedBy(spanMillis));
  }

  private Chart chart(UnresolvedExpression newAggregationFunction) {
    return this.toBuilder().aggregationFunction(newAggregationFunction).build();
  }

  @RequiredArgsConstructor
  static class PerFunction {
    private static final Map<String, Integer> UNIT_SECONDS =
        Map.of(
            "per_second", 1,
            "per_minute", 60,
            "per_hour", 3600,
            "per_day", 86400);
    private final String aggName;
    private final UnresolvedExpression aggArg;
    private final int seconds;

    static Optional<PerFunction> from(UnresolvedExpression aggExpr) {
        if (aggExpr instanceof Alias) {
          return from(((Alias) aggExpr).getDelegated());
      };
      if (!(aggExpr instanceof AggregateFunction)) {
        return Optional.empty();
      }

      AggregateFunction aggFunc = (AggregateFunction) aggExpr;
      String aggFuncName = aggFunc.getFuncName().toLowerCase(Locale.ROOT);
      if (!UNIT_SECONDS.containsKey(aggFuncName)) {
        return Optional.empty();
      }

      String aggName = toAggName(aggFunc);
      return Optional.of(
          new PerFunction(aggName, aggFunc.getField(), UNIT_SECONDS.get(aggFuncName)));
    }

    private static String toAggName(AggregateFunction aggFunc) {
      String fieldName =
          (aggFunc.getField() instanceof Field)
              ? ((Field) aggFunc.getField()).getField().toString()
              : aggFunc.getField().toString();
      return String.format(Locale.ROOT, "%s(%s)", aggFunc.getFuncName(), fieldName);
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
