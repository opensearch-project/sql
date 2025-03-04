/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.ast.expression.SpanUnit.NONE;
import static org.opensearch.sql.ast.expression.SpanUnit.UNKNOWN;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.BuiltinFunctionUtils;
import org.opensearch.sql.exception.SemanticCheckException;

@RequiredArgsConstructor
public class CalciteRexNodeVisitor extends AbstractNodeVisitor<RexNode, CalcitePlanContext> {
  private final CalciteRelNodeVisitor planVisitor;

  public RexNode analyze(UnresolvedExpression unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  public RexNode analyzeJoinCondition(UnresolvedExpression unresolved, CalcitePlanContext context) {
    return context.resolveJoinCondition(unresolved, this::analyze);
  }

  @Override
  public RexNode visitLiteral(Literal node, CalcitePlanContext context) {
    RexBuilder rexBuilder = context.rexBuilder;
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    final Object value = node.getValue();
    if (value == null) {
      final RelDataType type = typeFactory.createSqlType(SqlTypeName.NULL);
      return rexBuilder.makeNullLiteral(type);
    }
    switch (node.getType()) {
      case NULL:
        return rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
      case STRING:
        return rexBuilder.makeLiteral(value.toString());
      case INTEGER:
        return rexBuilder.makeExactLiteral(new BigDecimal((Integer) value));
      case LONG:
        return rexBuilder.makeBigintLiteral(new BigDecimal((Long) value));
      case SHORT:
        return rexBuilder.makeExactLiteral(
            new BigDecimal((Short) value), typeFactory.createSqlType(SqlTypeName.SMALLINT));
      case FLOAT:
        return rexBuilder.makeApproxLiteral(
            new BigDecimal(Float.toString((Float) value)),
            typeFactory.createSqlType(SqlTypeName.FLOAT));
      case DOUBLE:
        return rexBuilder.makeApproxLiteral(
            new BigDecimal(Double.toString((Double) value)),
            typeFactory.createSqlType(SqlTypeName.DOUBLE));
      case BOOLEAN:
        return rexBuilder.makeLiteral((Boolean) value);
      case DATE:
        return rexBuilder.makeDateLiteral(new DateString(value.toString()));
      case TIME:
        return rexBuilder.makeTimeLiteral(
            new TimeString(value.toString()), RelDataType.PRECISION_NOT_SPECIFIED);
      case TIMESTAMP:
        return rexBuilder.makeTimestampLiteral(
            new TimestampString(value.toString()), RelDataType.PRECISION_NOT_SPECIFIED);
      case INTERVAL:
        //                return rexBuilder.makeIntervalLiteral(BigDecimal.valueOf((long)
        // node.getValue()));
      default:
        throw new UnsupportedOperationException("Unsupported literal type: " + node.getType());
    }
  }

  @Override
  public RexNode visitAnd(And node, CalcitePlanContext context) {
    final RelDataType booleanType =
        context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.rexBuilder.makeCall(booleanType, SqlStdOperatorTable.AND, List.of(left, right));
  }

  @Override
  public RexNode visitOr(Or node, CalcitePlanContext context) {
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.relBuilder.or(left, right);
  }

  @Override
  public RexNode visitXor(Xor node, CalcitePlanContext context) {
    final RelDataType booleanType =
        context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.rexBuilder.makeCall(
        booleanType, SqlStdOperatorTable.BIT_XOR, List.of(left, right));
  }

  @Override
  public RexNode visitNot(Not node, CalcitePlanContext context) {
    final RexNode expr = analyze(node.getExpression(), context);
    return context.relBuilder.not(expr);
  }

  @Override
  public RexNode visitCompare(Compare node, CalcitePlanContext context) {
    final RelDataType booleanType =
        context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.rexBuilder.makeCall(
        booleanType, BuiltinFunctionUtils.translate(node.getOperator()), List.of(left, right));
  }

  @Override
  public RexNode visitEqualTo(EqualTo node, CalcitePlanContext context) {
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.rexBuilder.equals(left, right);
  }

  @Override
  public RexNode visitQualifiedName(QualifiedName node, CalcitePlanContext context) {
    if (context.isResolvingJoinCondition()) {
      List<String> parts = node.getParts();
      if (parts.size() == 1) {
        // Handle the case of `id = cid`
        try {
          // TODO what if there is join clause in InSubquery in join condition
          // for subquery in join condition
          return context.relBuilder.field(parts.get(0));
        } catch (IllegalArgumentException e) {
          try {
            return context.relBuilder.field(2, 0, parts.get(0));
          } catch (IllegalArgumentException ee) {
            return context.relBuilder.field(2, 1, parts.get(0));
          }
        }
      } else if (parts.size()
          == 2) { // Handle the case of `t1.id = t2.id` or `alias1.id = alias2.id`
        return context.relBuilder.field(2, parts.get(0), parts.get(1));
      } else if (parts.size() == 3) {
        throw new UnsupportedOperationException("Unsupported qualified name: " + node);
      }
    }
    String qualifiedName = node.toString();
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    if (currentFields.contains(qualifiedName)) {
      return context.relBuilder.field(qualifiedName);
    } else if (node.getParts().size() == 2) {
      List<String> parts = node.getParts();
      return context.relBuilder.field(1, parts.get(0), parts.get(1));
    } else if (currentFields.stream().noneMatch(f -> f.startsWith(qualifiedName))) {
      return context.relBuilder.field(qualifiedName);
    }
    // Handle the overriding fields, for example, `eval SAL = SAL + 1` will delete the original SAL
    // and add a SAL0
    Map<String, String> fieldMap =
        currentFields.stream().collect(Collectors.toMap(s -> s.replaceAll("\\d", ""), s -> s));
    if (fieldMap.containsKey(qualifiedName)) {
      return context.relBuilder.field(fieldMap.get(qualifiedName));
    } else {
      return null;
    }
  }

  @Override
  public RexNode visitAlias(Alias node, CalcitePlanContext context) {
    RexNode expr = analyze(node.getDelegated(), context);
    return context.relBuilder.alias(expr, node.getName());
  }

  @Override
  public RexNode visitSpan(Span node, CalcitePlanContext context) {
    RexNode field = analyze(node.getField(), context);
    RexNode value = analyze(node.getValue(), context);
    RelDataTypeFactory typeFactory = context.rexBuilder.getTypeFactory();
    SpanUnit unit = node.getUnit();
    if (isTimeBased(unit)) {
      SqlIntervalQualifier intervalQualifier = context.rexBuilder.createIntervalUntil(unit);
      long millis = SqlParserUtil.intervalToMillis(value.toString(), intervalQualifier);
      return context.rexBuilder.makeIntervalLiteral(new BigDecimal(millis), intervalQualifier);
    } else {
      // if the unit is not time base - create a math expression to bucket the span partitions
      return context.rexBuilder.makeCall(
          typeFactory.createSqlType(SqlTypeName.DOUBLE),
          SqlStdOperatorTable.MULTIPLY,
          List.of(
              context.rexBuilder.makeCall(
                  typeFactory.createSqlType(SqlTypeName.DOUBLE),
                  SqlStdOperatorTable.FLOOR,
                  List.of(
                      context.rexBuilder.makeCall(
                          typeFactory.createSqlType(SqlTypeName.DOUBLE),
                          SqlStdOperatorTable.DIVIDE,
                          List.of(field, value)))),
              value));
    }
  }

  private boolean isTimeBased(SpanUnit unit) {
    return !(unit == NONE || unit == UNKNOWN);
  }

  //    @Override
  //    public RexNode visitAggregateFunction(AggregateFunction node, Context context) {
  //        RexNode field = analyze(node.getField(), context);
  //        AggregateCall aggregateCall = translateAggregateCall(node, field, relBuilder);
  //        return new MyAggregateCall(aggregateCall);
  //    }

  @Override
  public RexNode visitLet(Let node, CalcitePlanContext context) {
    RexNode expr = analyze(node.getExpression(), context);
    return context.relBuilder.alias(expr, node.getVar().getField().toString());
  }

  @Override
  public RexNode visitFunction(Function node, CalcitePlanContext context) {
    List<RexNode> arguments =
        node.getFuncArgs().stream().map(arg -> analyze(arg, context)).collect(Collectors.toList());
    return context.rexBuilder.makeCall(
        BuiltinFunctionUtils.translate(node.getFuncName()), arguments);
  }

  @Override
  public RexNode visitInSubquery(InSubquery node, CalcitePlanContext context) {
    List<RexNode> nodes = node.getChild().stream().map(child -> analyze(child, context)).toList();
    UnresolvedPlan subquery = node.getQuery();
    RelNode subqueryRel = subquery.accept(planVisitor, context);
    context.relBuilder.build();
    try {
      return context.relBuilder.in(subqueryRel, nodes);
      // TODO
      // The {@link org.apache.calcite.tools.RelBuilder#in(RexNode,java.util.function.Function)}
      // only support one expression. Change to follow code after calcite fixed.
      //    return context.relBuilder.in(
      //        nodes.getFirst(),
      //        b -> {
      //          RelNode subqueryRel = subquery.accept(planVisitor, context);
      //          b.build();
      //          return subqueryRel;
      //        });
    } catch (AssertionError e) {
      throw new SemanticCheckException(
          "The number of columns in the left hand side of an IN subquery does not match the number"
              + " of columns in the output of subquery");
    }
  }
}
