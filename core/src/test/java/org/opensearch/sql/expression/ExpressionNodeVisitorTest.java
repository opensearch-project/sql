/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.conditional.cases.CaseClause;
import org.opensearch.sql.expression.conditional.cases.WhenClause;
import org.opensearch.sql.expression.parse.ParseExpression;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ExpressionNodeVisitorTest {

  @Test
  void should_return_null_by_default() {
    ExpressionNodeVisitor<Object, Object> visitor = new ExpressionNodeVisitor<Object, Object>() {};
    assertNull(new HighlightExpression(DSL.literal("Title")).accept(visitor, null));
    assertNull(literal(10).accept(visitor, null));
    assertNull(ref("name", STRING).accept(visitor, null));
    assertNull(named("bool", literal(true)).accept(visitor, null));
    assertNull(DSL.abs(literal(-10)).accept(visitor, null));
    assertNull(DSL.sum(literal(10)).accept(visitor, null));
    assertNull(
        named("avg", new AvgAggregator(List.of(ref("age", INTEGER)), INTEGER))
            .accept(visitor, null));
    assertNull(new CaseClause(ImmutableList.of(), null).accept(visitor, null));
    assertNull(new WhenClause(literal("test"), literal(10)).accept(visitor, null));
    assertNull(DSL.namedArgument("field", literal("message")).accept(visitor, null));
    assertNull(DSL.span(ref("age", INTEGER), literal(1), "").accept(visitor, null));
    assertNull(
        DSL.regex(ref("name", STRING), DSL.literal("(?<group>\\d+)"), DSL.literal("group"))
            .accept(visitor, null));
  }

  @Test
  void can_visit_all_types_of_expression_node() {
    Expression expr =
        DSL.regex(
            DSL.castString(DSL.sum(DSL.add(ref("balance", INTEGER), literal(10)))),
            DSL.literal("(?<group>\\d+)"),
            DSL.literal("group"));

    Expression actual =
        expr.accept(
            new ExpressionNodeVisitor<Expression, Object>() {
              @Override
              public Expression visitLiteral(LiteralExpression node, Object context) {
                return node;
              }

              @Override
              public Expression visitReference(ReferenceExpression node, Object context) {
                return node;
              }

              @Override
              public Expression visitParse(ParseExpression node, Object context) {
                return node;
              }

              @Override
              public Expression visitFunction(FunctionExpression node, Object context) {
                return DSL.add(visitArguments(node.getArguments(), context));
              }

              @Override
              public Expression visitAggregator(Aggregator<?> node, Object context) {
                return DSL.sum(visitArguments(node.getArguments(), context));
              }

              private Expression[] visitArguments(List<Expression> arguments, Object context) {
                return arguments.stream()
                    .map(arg -> arg.accept(this, context))
                    .toArray(Expression[]::new);
              }
            },
            null);

    assertEquals(expr, actual);
  }
}
