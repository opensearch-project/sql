/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.planner.converter;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.literal;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.of;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.ref;
import static org.opensearch.sql.legacy.expression.core.operator.ScalarOperation.ADD;
import static org.opensearch.sql.legacy.expression.core.operator.ScalarOperation.LOG;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.integerValue;

import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.legacy.expression.core.Expression;
import org.opensearch.sql.legacy.query.planner.converter.SQLAggregationParser;
import org.opensearch.sql.legacy.query.planner.converter.SQLExprToExpressionConverter;

@RunWith(MockitoJUnitRunner.class)
public class SQLExprToExpressionConverterTest {
  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

  private SQLExprToExpressionConverter converter;
  private SQLAggregationParser.Context context;
  private final SQLAggregateExpr maxA = new SQLAggregateExpr("MAX");
  private final SQLAggregateExpr maxB = new SQLAggregateExpr("MAX");
  private final SQLAggregateExpr minA = new SQLAggregateExpr("MIN");
  private final SQLIdentifierExpr groupG = new SQLIdentifierExpr("A");
  private final SQLIdentifierExpr aggA = new SQLIdentifierExpr("A");
  private final SQLIdentifierExpr aggB = new SQLIdentifierExpr("B");
  private final SQLIntegerExpr one = new SQLIntegerExpr(1);

  @Before
  public void setup() {
    maxA.getArguments().add(aggA);
    maxB.getArguments().add(aggB);
    minA.getArguments().add(aggA);
    context = new SQLAggregationParser.Context(ImmutableMap.of());
    converter = new SQLExprToExpressionConverter(context);
  }

  @Test
  public void identifierShouldReturnVarExpression() {
    context.addGroupKeyExpr(groupG);
    Expression expression = converter.convert(groupG);

    assertEquals(ref("A").toString(), expression.toString());
  }

  @Test
  public void binaryOperatorAddShouldReturnAddExpression() {
    context.addAggregationExpr(maxA);
    context.addAggregationExpr(minA);

    Expression expression =
        converter.convert(new SQLBinaryOpExpr(maxA, SQLBinaryOperator.Add, minA));
    assertEquals(add(ref("MAX_0"), ref("MIN_1")).toString(), expression.toString());
  }

  @Test
  public void compoundBinaryOperatorShouldReturnCorrectExpression() {
    context.addAggregationExpr(maxA);
    context.addAggregationExpr(minA);

    Expression expression =
        converter.convert(
            new SQLBinaryOpExpr(
                maxA,
                SQLBinaryOperator.Add,
                new SQLBinaryOpExpr(maxA, SQLBinaryOperator.Add, minA)));
    assertEquals(
        add(ref("MAX_0"), add(ref("MAX_0"), ref("MIN_1"))).toString(), expression.toString());
  }

  @Test
  public void functionOverCompoundBinaryOperatorShouldReturnCorrectExpression() {
    context.addAggregationExpr(maxA);
    context.addAggregationExpr(minA);

    SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr("LOG");
    methodInvokeExpr.addParameter(
        new SQLBinaryOpExpr(
            maxA, SQLBinaryOperator.Add, new SQLBinaryOpExpr(maxA, SQLBinaryOperator.Add, minA)));

    Expression expression = converter.convert(methodInvokeExpr);
    assertEquals(
        log(add(ref("MAX_0"), add(ref("MAX_0"), ref("MIN_1")))).toString(), expression.toString());
  }

  @Test
  public void functionOverGroupColumn() {
    context.addAggregationExpr(maxA);
    context.addAggregationExpr(minA);

    SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr("LOG");
    methodInvokeExpr.addParameter(
        new SQLBinaryOpExpr(
            maxA, SQLBinaryOperator.Add, new SQLBinaryOpExpr(maxA, SQLBinaryOperator.Add, minA)));

    Expression expression = converter.convert(methodInvokeExpr);
    assertEquals(
        log(add(ref("MAX_0"), add(ref("MAX_0"), ref("MIN_1")))).toString(), expression.toString());
  }

  @Test
  public void binaryOperatorWithLiteralAddShouldReturnAddExpression() {
    context.addAggregationExpr(maxA);

    Expression expression =
        converter.convert(new SQLBinaryOpExpr(maxA, SQLBinaryOperator.Add, one));
    assertEquals(add(ref("MAX_0"), literal(integerValue(1))).toString(), expression.toString());
  }

  @Test
  public void unknownIdentifierShouldThrowException() {
    context.addAggregationExpr(maxA);
    context.addAggregationExpr(minA);

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("unsupported expr");
    converter.convert(new SQLBinaryOpExpr(maxA, SQLBinaryOperator.Add, maxB));
  }

  @Test
  public void unsupportOperationShouldThrowException() {
    exceptionRule.expect(UnsupportedOperationException.class);
    exceptionRule.expectMessage("unsupported operator: cot");

    context.addAggregationExpr(maxA);
    SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr("cot");
    methodInvokeExpr.addParameter(maxA);
    converter.convert(methodInvokeExpr);
  }

  private Expression add(Expression... expressions) {
    return of(ADD, Arrays.asList(expressions));
  }

  private Expression log(Expression... expressions) {
    return of(LOG, Arrays.asList(expressions));
  }
}
