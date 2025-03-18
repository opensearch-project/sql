/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.ast.expression.SpanUnit.NONE;
import static org.opensearch.sql.ast.expression.SpanUnit.UNKNOWN;
import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.translateArgument;

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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.BuiltinFunctionUtils;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.CalciteUnsupportedException;
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
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.relBuilder.notEquals(left, right);
  }

  @Override
  public RexNode visitNot(Not node, CalcitePlanContext context) {
    final RexNode expr = analyze(node.getExpression(), context);
    return context.relBuilder.not(expr);
  }

  @Override
  public RexNode visitCompare(Compare node, CalcitePlanContext context) {
    SqlOperator op = BuiltinFunctionUtils.translate(node.getOperator());
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.relBuilder.call(op, left, right);
  }

  @Override
  public RexNode visitBetween(Between node, CalcitePlanContext context) {
    RexNode value = analyze(node.getValue(), context);
    RexNode lowerBound = analyze(node.getLowerBound(), context);
    RexNode upperBound = analyze(node.getUpperBound(), context);
    RelDataType commonType = context.rexBuilder.commonType(value, lowerBound, upperBound);
    if (commonType != null) {
      lowerBound = context.rexBuilder.makeCast(commonType, lowerBound);
      upperBound = context.rexBuilder.makeCast(commonType, upperBound);
    } else {
      throw new SemanticCheckException(
          StringUtils.format(
              "BETWEEN expression types are incompatible: [%s, %s, %s]",
              value.getType(), lowerBound.getType(), upperBound.getType()));
    }
    return context.relBuilder.between(value, lowerBound, upperBound);
  }

  @Override
  public RexNode visitEqualTo(EqualTo node, CalcitePlanContext context) {
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.rexBuilder.equals(left, right);
  }

  /** Resolve qualified name. Note, the name should be case-sensitive. */
  @Override
  public RexNode visitQualifiedName(QualifiedName node, CalcitePlanContext context) {
    // 1. resolve QualifiedName in join condition
    if (context.isResolvingJoinCondition()) {
      List<String> parts = node.getParts();
      if (parts.size() == 1) {
        // 1.1 Handle the case of `id = cid`
        try {
          return context.relBuilder.field(2, 0, parts.getFirst());
        } catch (IllegalArgumentException ee) {
          return context.relBuilder.field(2, 1, parts.getFirst());
        }
      } else if (parts.size() == 2) {
        // 1.2 Handle the case of `t1.id = t2.id` or `alias1.id = alias2.id`
        return context.relBuilder.field(2, parts.get(0), parts.get(1));
      } else if (parts.size() == 3) {
        throw new UnsupportedOperationException("Unsupported qualified name: " + node);
      }
    }

    // 2. resolve QualifiedName in non-join condition
    String qualifiedName = node.toString();
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    if (currentFields.contains(qualifiedName)) {
      // 2.1 resolve QualifiedName from stack top
      return context.relBuilder.field(qualifiedName);
    } else if (node.getParts().size() == 2) {
      // 2.2 resolve QualifiedName with an alias or table name
      List<String> parts = node.getParts();
      try {
        return context.relBuilder.field(1, parts.get(0), parts.get(1));
      } catch (IllegalArgumentException e) {
        // 2.3 resolve QualifiedName with outer alias
        return context
            .peekCorrelVar()
            .map(correlVar -> context.relBuilder.field(correlVar, parts.get(1)))
            .orElseThrow(() -> e); // Re-throw the exception if no correlated variable exists
      }
    } else if (currentFields.stream().noneMatch(f -> f.startsWith(qualifiedName))) {
      // 2.4 try resolving combination of 2.1 and 2.3 to resolve rest cases
      return context
          .peekCorrelVar()
          .map(correlVar -> context.relBuilder.field(correlVar, qualifiedName))
          .orElseGet(() -> context.relBuilder.field(qualifiedName));
    }
    // 3. resolve overriding fields, for example, `eval SAL = SAL + 1` will delete the original SAL
    // and add a SAL0. SAL0 in currentFields, but qualifiedName is SAL.
    // TODO now we cannot handle the case using a overriding fields in subquery, for example
    // source = EMP | eval DEPTNO = DEPTNO + 1 | where exists [ source = DEPT | where emp.DEPTNO =
    // DEPTNO ]
    Map<String, String> fieldMap =
        currentFields.stream().collect(Collectors.toMap(s -> s.replaceAll("\\d", ""), s -> s));
    if (fieldMap.containsKey(qualifiedName)) {
      return context.relBuilder.field(fieldMap.get(qualifiedName));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "field [%s] not found; input fields are: %s", qualifiedName, currentFields));
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
      return context.rexBuilder.makeCall(
          BuiltinFunctionUtils.translate("SPAN"),
          List.of(
              field,
              context
                  .relBuilder
                  .getRexBuilder()
                  .makeLiteral(field.getType().getSqlTypeName().getName()),
              value,
              context.relBuilder.getRexBuilder().makeLiteral(unit.getName())));
    } else {
      // if the unit is not time base - create a math expression to bucket the span partitions
      SqlTypeName type = field.getType().getSqlTypeName();
      return context.rexBuilder.makeCall(
          typeFactory.createSqlType(type),
          SqlStdOperatorTable.MULTIPLY,
          List.of(
              context.rexBuilder.makeCall(
                  typeFactory.createSqlType(type),
                  SqlStdOperatorTable.FLOOR,
                  List.of(
                      context.rexBuilder.makeCall(
                          typeFactory.createSqlType(type),
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
        BuiltinFunctionUtils.translate(node.getFuncName()),
        translateArgument(node.getFuncName(), arguments, context));
  }

  @Override
  public RexNode visitInSubquery(InSubquery node, CalcitePlanContext context) {
    List<RexNode> nodes = node.getChild().stream().map(child -> analyze(child, context)).toList();
    UnresolvedPlan subquery = node.getQuery();
    RelNode subqueryRel = resolveSubqueryPlan(subquery, context);
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

  @Override
  public RexNode visitScalarSubquery(ScalarSubquery node, CalcitePlanContext context) {
    return context.relBuilder.scalarQuery(
        b -> {
          UnresolvedPlan subquery = node.getQuery();
          return resolveSubqueryPlan(subquery, context);
        });
  }

  @Override
  public RexNode visitExistsSubquery(ExistsSubquery node, CalcitePlanContext context) {
    return context.relBuilder.exists(
        b -> {
          UnresolvedPlan subquery = node.getQuery();
          return resolveSubqueryPlan(subquery, context);
        });
  }

  private RelNode resolveSubqueryPlan(UnresolvedPlan subquery, CalcitePlanContext context) {
    // clear and store the outer state
    boolean isResolvingJoinConditionOuter = context.isResolvingJoinCondition();
    if (isResolvingJoinConditionOuter) {
      context.setResolvingJoinCondition(false);
    }
    RelNode subqueryRel = subquery.accept(planVisitor, context);
    // pop the inner plan
    context.relBuilder.build();
    // clear the exists subquery resolving state
    // restore to the previous state
    if (isResolvingJoinConditionOuter) {
      context.setResolvingJoinCondition(true);
    }
    return subqueryRel;
  }

  /*
   * Unsupported Expressions of PPL with Calcite for OpenSearch 3.0.0-beta
   */
  @Override
  public RexNode visitCast(Cast node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("CastWhen function is unsupported in Calcite");
  }

  @Override
  public RexNode visitWhen(When node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("CastWhen function is unsupported in Calcite");
  }

  @Override
  public RexNode visitRelevanceFieldList(RelevanceFieldList node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Relevance fields expression is unsupported in Calcite");
  }
}
