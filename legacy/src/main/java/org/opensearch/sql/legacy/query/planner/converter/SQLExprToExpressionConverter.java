/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.converter;

import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.cast;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.literal;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.expression.core.Expression;
import org.opensearch.sql.legacy.expression.core.ExpressionFactory;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperation;
import org.opensearch.sql.legacy.expression.model.ExprValueFactory;

/** The definition of {@link SQLExpr} to {@link Expression} converter. */
@RequiredArgsConstructor
public class SQLExprToExpressionConverter {
  private static final Map<SQLBinaryOperator, ScalarOperation> binaryOperatorOperationMap =
      new ImmutableMap.Builder<SQLBinaryOperator, ScalarOperation>()
          .put(SQLBinaryOperator.Add, ScalarOperation.ADD)
          .put(SQLBinaryOperator.Subtract, ScalarOperation.SUBTRACT)
          .put(SQLBinaryOperator.Multiply, ScalarOperation.MULTIPLY)
          .put(SQLBinaryOperator.Divide, ScalarOperation.DIVIDE)
          .put(SQLBinaryOperator.Modulus, ScalarOperation.MODULES)
          .build();
  private static final Map<String, ScalarOperation> methodOperationMap =
      new ImmutableMap.Builder<String, ScalarOperation>()
          .put(ScalarOperation.ABS.getName(), ScalarOperation.ABS)
          .put(ScalarOperation.ACOS.getName(), ScalarOperation.ACOS)
          .put(ScalarOperation.ASIN.getName(), ScalarOperation.ASIN)
          .put(ScalarOperation.ATAN.getName(), ScalarOperation.ATAN)
          .put(ScalarOperation.ATAN2.getName(), ScalarOperation.ATAN2)
          .put(ScalarOperation.TAN.getName(), ScalarOperation.TAN)
          .put(ScalarOperation.CBRT.getName(), ScalarOperation.CBRT)
          .put(ScalarOperation.CEIL.getName(), ScalarOperation.CEIL)
          .put(ScalarOperation.COS.getName(), ScalarOperation.COS)
          .put(ScalarOperation.COSH.getName(), ScalarOperation.COSH)
          .put(ScalarOperation.EXP.getName(), ScalarOperation.EXP)
          .put(ScalarOperation.FLOOR.getName(), ScalarOperation.FLOOR)
          .put(ScalarOperation.LN.getName(), ScalarOperation.LN)
          .put(ScalarOperation.LOG.getName(), ScalarOperation.LOG)
          .put(ScalarOperation.LOG2.getName(), ScalarOperation.LOG2)
          .put(ScalarOperation.LOG10.getName(), ScalarOperation.LOG10)
          .build();

  private final SQLAggregationParser.Context context;

  /**
   * Convert the {@link SQLExpr} to {@link Expression}
   *
   * @param expr {@link SQLExpr}
   * @return expression {@link Expression}
   */
  public Expression convert(SQLExpr expr) {
    Optional<Expression> resolvedExpression = context.resolve(expr);
    if (resolvedExpression.isPresent()) {
      return resolvedExpression.get();
    } else {
      if (expr instanceof SQLBinaryOpExpr) {
        return binaryOperatorToExpression((SQLBinaryOpExpr) expr, this::convert);
      } else if (expr instanceof SQLMethodInvokeExpr) {
        return methodToExpression((SQLMethodInvokeExpr) expr, this::convert);
      } else if (expr instanceof SQLValuableExpr) {
        return literal(ExprValueFactory.from(((SQLValuableExpr) expr).getValue()));
      } else if (expr instanceof SQLCastExpr) {
        return cast(convert(((SQLCastExpr) expr).getExpr()));
      } else {
        throw new RuntimeException("unsupported expr: " + expr);
      }
    }
  }

  private Expression binaryOperatorToExpression(
      SQLBinaryOpExpr expr, Function<SQLExpr, Expression> converter) {
    if (binaryOperatorOperationMap.containsKey(expr.getOperator())) {
      return ExpressionFactory.of(
          binaryOperatorOperationMap.get(expr.getOperator()),
          Arrays.asList(converter.apply(expr.getLeft()), converter.apply(expr.getRight())));
    } else {
      throw new UnsupportedOperationException(
          "unsupported operator: " + expr.getOperator().getName());
    }
  }

  private Expression methodToExpression(
      SQLMethodInvokeExpr expr, Function<SQLExpr, Expression> converter) {
    String methodName = expr.getMethodName().toLowerCase();
    if (methodOperationMap.containsKey(methodName)) {

      return ExpressionFactory.of(
          methodOperationMap.get(methodName),
          expr.getParameters().stream().map(converter).collect(Collectors.toList()));
    } else {
      throw new UnsupportedOperationException("unsupported operator: " + expr.getMethodName());
    }
  }
}
