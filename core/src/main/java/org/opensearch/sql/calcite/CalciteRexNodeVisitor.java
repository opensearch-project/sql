/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.SqlKind.AS;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.opensearch.sql.ast.expression.SpanUnit.NONE;
import static org.opensearch.sql.ast.expression.SpanUnit.UNKNOWN;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

@RequiredArgsConstructor
public class CalciteRexNodeVisitor extends AbstractNodeVisitor<RexNode, CalcitePlanContext> {
  private final CalciteRelNodeVisitor planVisitor;

  public RexNode analyze(UnresolvedExpression unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  public List<RexNode> analyze(List<UnresolvedExpression> list, CalcitePlanContext context) {
    return list.stream().map(u -> u.accept(this, context)).toList();
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
        if (value.toString().length() == 1) {
          // To align Spark/PostgreSQL, Char(1) is useful, such as cast('1' to boolean) should
          // return true
          return rexBuilder.makeLiteral(
              value.toString(), typeFactory.createSqlType(SqlTypeName.CHAR));
        } else {
          // Specific the type to VARCHAR and allowCast to true, or the STRING will be optimized to
          // CHAR(n)
          // which leads to incorrect return type in deriveReturnType of some functions/operators
          return rexBuilder.makeLiteral(
              value.toString(), typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        }
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
      case DECIMAL:
        return rexBuilder.makeExactLiteral((BigDecimal) value);
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
      default:
        throw new UnsupportedOperationException("Unsupported literal type: " + node.getType());
    }
  }

  @Override
  public RexNode visitInterval(Interval node, CalcitePlanContext context) {
    RexNode value = analyze(node.getValue(), context);
    SqlIntervalQualifier intervalQualifier =
        context.rexBuilder.createIntervalUntil(PlanUtils.intervalUnitToSpanUnit(node.getUnit()));
    return context.rexBuilder.makeIntervalLiteral(
        new BigDecimal(value.toString()), intervalQualifier);
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
  public RexNode visitIn(In node, CalcitePlanContext context) {
    final RexNode field = analyze(node.getField(), context);
    final List<RexNode> valueList =
        node.getValueList().stream().map(value -> analyze(value, context)).toList();
    final List<RelDataType> dataTypes =
        new java.util.ArrayList<>(valueList.stream().map(RexNode::getType).toList());
    dataTypes.add(field.getType());
    RelDataType commonType = context.rexBuilder.getTypeFactory().leastRestrictive(dataTypes);
    if (commonType != null) {
      List<RexNode> newValueList =
          valueList.stream().map(value -> context.rexBuilder.makeCast(commonType, value)).toList();
      return context.rexBuilder.makeIn(field, newValueList);
    } else {
      List<ExprType> exprTypes =
          dataTypes.stream().map(OpenSearchTypeFactory::convertRelDataTypeToExprType).toList();
      throw new SemanticCheckException(
          StringUtils.format(
              "In expression types are incompatible: fields type %s, values type %s",
              exprTypes.getLast(), exprTypes.subList(0, exprTypes.size() - 1)));
    }
  }

  @Override
  public RexNode visitCompare(Compare node, CalcitePlanContext context) {
    RexNode left = analyze(node.getLeft(), context);
    RexNode right = analyze(node.getRight(), context);
    return PPLFuncImpTable.INSTANCE.resolve(context.rexBuilder, node.getOperator(), left, right);
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
              OpenSearchTypeFactory.convertRelDataTypeToExprType(value.getType()),
              OpenSearchTypeFactory.convertRelDataTypeToExprType(lowerBound.getType()),
              OpenSearchTypeFactory.convertRelDataTypeToExprType(upperBound.getType())));
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
        try {
          return context.relBuilder.field(2, parts.get(0), parts.get(1));
        } catch (IllegalArgumentException e) {
          // Similar to the step 2.3.
          List<String> candidates =
              context.relBuilder.peek(1).getRowType().getFieldNames().stream()
                  .filter(col -> substringAfterLast(col, ".").equals(parts.getLast()))
                  .toList();
          for (String candidate : candidates) {
            try {
              // field("nation2", "n2.n_name"); // pass
              return context.relBuilder.field(2, parts.get(0), candidate);
            } catch (IllegalArgumentException e1) {
              // field("nation2", "n_name"); // do nothing when fail (n_name is field of nation1)
            }
          }
          throw new UnsupportedOperationException("Unsupported qualified name: " + node);
        }
      } else if (parts.size() == 3) {
        throw new UnsupportedOperationException("Unsupported qualified name: " + node);
      }
    }

    // TODO: Need to support nested fields https://github.com/opensearch-project/sql/issues/3459
    // 2. resolve QualifiedName in non-join condition
    String qualifiedName = node.toString();
    if (context.getRexLambdaRefMap().containsKey(qualifiedName)) {
      return context.getRexLambdaRefMap().get(qualifiedName);
    }
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    if (currentFields.contains(qualifiedName)) {
      // 2.1 resolve QualifiedName from stack top
      // Note: QualifiedName with multiple parts also could be applied in step 2.1,
      // for example `n2.n_name` or `nation2.n_name` in the output of join can be resolved here.
      return context.relBuilder.field(qualifiedName);
    } else if (node.getParts().size() == 2) {
      // 2.2 resolve QualifiedName with an alias or table name
      List<String> parts = node.getParts();
      try {
        return context.relBuilder.field(1, parts.get(0), parts.get(1));
      } catch (IllegalArgumentException e) {
        // 2.3 For field which renamed with <alias.field>, to resolve the field with table
        // identifier
        // `nation2.n_name`,
        // we convert it to resolve <table.alias.field>, e.g. `nation2.n2.n_name`
        // `n2.n_name` was the renamed field name from the duplicated field `(nation2.)n_name0` of
        // join output.
        // Build the candidates which contains `n_name`: e.g. `(nation1.)n_name`, `n2.n_name`
        List<String> candidates =
            context.relBuilder.peek().getRowType().getFieldNames().stream()
                .filter(col -> substringAfterLast(col, ".").equals(parts.getLast()))
                .toList();
        for (String candidate : candidates) {
          try {
            // field("nation2", "n2.n_name"); // pass
            return context.relBuilder.field(parts.get(0), candidate);
          } catch (IllegalArgumentException e1) {
            // field("nation2", "n_name"); // do nothing when fail (n_name is field of nation1)
          }
        }
        // 2.4 resolve QualifiedName with outer alias
        // check existing of parts.get(0)
        return context
            .peekCorrelVar()
            .map(correlVar -> context.relBuilder.field(correlVar, parts.get(1)))
            .orElseThrow(() -> e); // Re-throw the exception if no correlated variable exists
      }
    } else if (currentFields.stream().noneMatch(f -> f.startsWith(qualifiedName))) {
      // 2.5 try resolving combination of 2.1 and 2.4 to resolve rest cases
      return context
          .peekCorrelVar()
          .map(correlVar -> context.relBuilder.field(correlVar, qualifiedName))
          .orElseGet(() -> context.relBuilder.field(qualifiedName));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "field [%s] not found; input fields are: %s", qualifiedName, currentFields));
    }
  }

  @Override
  public RexNode visitAlias(Alias node, CalcitePlanContext context) {
    RexNode expr = analyze(node.getDelegated(), context);
    // Only OpenSearch SQL uses node.getAlias, OpenSearch PPL uses node.getName.
    return context.relBuilder.alias(
        expr, Strings.isEmpty(node.getAlias()) ? node.getName() : node.getAlias());
  }

  @Override
  public RexNode visitSpan(Span node, CalcitePlanContext context) {
    RexNode field = analyze(node.getField(), context);
    RexNode value = analyze(node.getValue(), context);
    SpanUnit unit = node.getUnit();
    RexBuilder rexBuilder = context.relBuilder.getRexBuilder();
    RexNode unitNode =
        isTimeBased(unit) ? rexBuilder.makeLiteral(unit.getName()) : rexBuilder.constantNull();
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder, BuiltinFunctionName.SPAN, field, value, unitNode);
  }

  private boolean isTimeBased(SpanUnit unit) {
    return !(unit == NONE || unit == UNKNOWN);
  }

  @Override
  public RexNode visitLambdaFunction(LambdaFunction node, CalcitePlanContext context) {
    try {
      List<QualifiedName> names = node.getFuncArgs();
      List<RexLambdaRef> args =
          IntStream.range(0, names.size())
              .mapToObj(
                  i ->
                      context.rexLambdaRefMap.getOrDefault(
                          names.get(i).toString(),
                          new RexLambdaRef(
                              i,
                              names.get(i).toString(),
                              TYPE_FACTORY.createSqlType(SqlTypeName.ANY))))
              .collect(Collectors.toList());
      RexNode body = node.getFunction().accept(this, context);
      RexNode lambdaNode = context.rexBuilder.makeLambdaCall(body, args);
      return lambdaNode;
    } catch (Exception e) {
      throw new RuntimeException("Cannot create lambda function", e);
    }
  }

  @Override
  public RexNode visitLet(Let node, CalcitePlanContext context) {
    RexNode expr = analyze(node.getExpression(), context);
    return context.relBuilder.alias(expr, node.getVar().getField().toString());
  }

  /**
   * The function will clone a context for lambda function. For lambda like (x, y, z) -> ..., we
   * will map type for each lambda argument by the order of previous argument. Also, the function
   * will add these variables to the context so they can pass visitQualifiedName
   */
  public CalcitePlanContext prepareLambdaContext(
      CalcitePlanContext context,
      LambdaFunction node,
      List<RexNode> previousArgument,
      String functionName,
      @Nullable RelDataType defaultTypeForReduceAcc) {
    try {
      CalcitePlanContext lambdaContext = context.clone();
      List<RelDataType> candidateType = new ArrayList<>();
      candidateType.add(
          ((ArraySqlType) previousArgument.get(0).getType())
              .getComponentType()); // The first argument should be array type
      candidateType.addAll(previousArgument.stream().skip(1).map(RexNode::getType).toList());
      candidateType =
          modifyLambdaTypeByFunction(functionName, candidateType, defaultTypeForReduceAcc);
      List<QualifiedName> argNames = node.getFuncArgs();
      Map<String, RexLambdaRef> lambdaTypes = new HashMap<>();
      int candidateIndex;
      candidateIndex = 0;
      for (int i = 0; i < argNames.size(); i++) {
        RelDataType type;
        if (candidateIndex < candidateType.size()) {
          type = candidateType.get(candidateIndex);
          candidateIndex++;
        } else {
          type =
              TYPE_FACTORY.createSqlType(
                  SqlTypeName.INTEGER); // For transform function, the i is missing in input.
        }
        lambdaTypes.put(
            argNames.get(i).toString(), new RexLambdaRef(i, argNames.get(i).toString(), type));
      }
      lambdaContext.putRexLambdaRefMap(lambdaTypes);
      return lambdaContext;
    } catch (Exception e) {
      throw new RuntimeException("Fail to prepare lambda context", e);
    }
  }

  /**
   * @param functionName function name
   * @param originalType the argument type by order
   * @return a modified types. Different functions need to implement its own order. Currently, only
   *     reduce has special logic.
   */
  private List<RelDataType> modifyLambdaTypeByFunction(
      String functionName,
      List<RelDataType> originalType,
      @Nullable RelDataType defaultTypeForReduceAcc) {
    switch (functionName.toUpperCase(Locale.ROOT)) {
      case "REDUCE": // For reduce case, the first type is acc should be any since it is the output
        // of accumulator lambda function
        if (originalType.size() == 2) {
          if (defaultTypeForReduceAcc != null) {
            return List.of(defaultTypeForReduceAcc, originalType.get(0));
          }
          return List.of(originalType.get(1), originalType.get(0));
        } else {
          return List.of(originalType.get(2));
        }
      default:
        return originalType;
    }
  }

  @Override
  public RexNode visitFunction(Function node, CalcitePlanContext context) {
    List<UnresolvedExpression> args = node.getFuncArgs();
    List<RexNode> arguments = new ArrayList<>();
    for (UnresolvedExpression arg : args) {
      if (arg instanceof LambdaFunction) {
        CalcitePlanContext lambdaContext =
            prepareLambdaContext(
                context, (LambdaFunction) arg, arguments, node.getFuncName(), null);
        RexNode lambdaNode = analyze(arg, lambdaContext);
        if (node.getFuncName().equalsIgnoreCase("reduce")) { // analyze again with calculate type
          lambdaContext =
              prepareLambdaContext(
                  context,
                  (LambdaFunction) arg,
                  arguments,
                  node.getFuncName(),
                  lambdaNode.getType());
          lambdaNode = analyze(arg, lambdaContext);
        }
        arguments.add(lambdaNode);
      } else {
        arguments.add(analyze(arg, context));
      }
    }

    RexNode resolvedNode =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, node.getFuncName(), arguments.toArray(new RexNode[0]));
    if (resolvedNode != null) {
      return resolvedNode;
    }
    throw new IllegalArgumentException("Unsupported operator: " + node.getFuncName());
  }

  @Override
  public RexNode visitWindowFunction(WindowFunction node, CalcitePlanContext context) {
    Function windowFunction = (Function) node.getFunction();
    List<RexNode> arguments =
        windowFunction.getFuncArgs().stream().map(arg -> analyze(arg, context)).toList();
    List<RexNode> partitions =
        node.getPartitionByList().stream()
            .map(arg -> analyze(arg, context))
            .map(this::extractRexNodeFromAlias)
            .toList();
    return BuiltinFunctionName.ofWindowFunction(windowFunction.getFuncName())
        .map(
            functionName -> {
              RexNode field = arguments.isEmpty() ? null : arguments.getFirst();
              List<RexNode> args =
                  (arguments.isEmpty() || arguments.size() == 1)
                      ? Collections.emptyList()
                      : arguments.subList(1, arguments.size());
              return PlanUtils.makeOver(
                  context, functionName, field, args, partitions, List.of(), node.getWindowFrame());
            })
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    "Unexpected window function: " + windowFunction.getFuncName()));
  }

  /** extract the expression of Alias from a node */
  private RexNode extractRexNodeFromAlias(RexNode node) {
    requireNonNull(node);
    if (node.getKind() == AS) {
      return ((RexCall) node).getOperands().get(0);
    } else {
      return node;
    }
  }

  @Override
  public RexNode visitInSubquery(InSubquery node, CalcitePlanContext context) {
    List<RexNode> nodes = node.getChild().stream().map(child -> analyze(child, context)).toList();
    UnresolvedPlan subquery = node.getQuery();
    RelNode subqueryRel = resolveSubqueryPlan(subquery, context);
    if (subqueryRel.getRowType().getFieldCount() != nodes.size()) {
      throw new SemanticCheckException(
          "The number of columns in the left hand side of an IN subquery does not match the number"
              + " of columns in the output of subquery");
    }
    // TODO
    //  The {@link org.apache.calcite.tools.RelBuilder#in(RexNode,java.util.function.Function)}
    //  only support one expression. Change to follow code after calcite fixed.
    //    return context.relBuilder.in(
    //        nodes.getFirst(),
    //        b -> {
    //          RelNode subqueryRel = subquery.accept(planVisitor, context);
    //          b.build();
    //          return subqueryRel;
    //        });
    return context.relBuilder.in(subqueryRel, nodes);
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
    boolean isNestedSubquery = context.isResolvingSubquery();
    context.setResolvingSubquery(true);
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
    // Only need to set isResolvingSubquery to false if it's not nested subquery.
    if (!isNestedSubquery) {
      context.setResolvingSubquery(false);
    }
    return subqueryRel;
  }

  @Override
  public RexNode visitCast(Cast node, CalcitePlanContext context) {
    RexNode expr = analyze(node.getExpression(), context);
    RelDataType type =
        OpenSearchTypeFactory.convertExprTypeToRelDataType(node.getDataType().getCoreType());
    RelDataType nullableType =
        context.rexBuilder.getTypeFactory().createTypeWithNullability(type, true);
    // call makeCast() instead of cast() because the saft parameter is true could avoid exception.
    return context.rexBuilder.makeCast(nullableType, expr, true, true);
  }

  @Override
  public RexNode visitCase(Case node, CalcitePlanContext context) {
    List<RexNode> caseOperands = new ArrayList<>();
    for (When when : node.getWhenClauses()) {
      caseOperands.add(analyze(when.getCondition(), context));
      caseOperands.add(analyze(when.getResult(), context));
    }
    RexNode elseExpr =
        node.getElseClause().map(e -> analyze(e, context)).orElse(context.relBuilder.literal(null));
    caseOperands.add(elseExpr);
    return context.rexBuilder.makeCall(SqlStdOperatorTable.CASE, caseOperands);
  }

  /*
   * Unsupported Expressions of PPL with Calcite for OpenSearch 3.0.0-beta
   */
  @Override
  public RexNode visitWhen(When node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("CastWhen function is unsupported in Calcite");
  }

  @Override
  public RexNode visitRelevanceFieldList(RelevanceFieldList node, CalcitePlanContext context) {
    List<RexNode> varArgRexNodeList = new ArrayList<>();
    node.getFieldList()
        .forEach(
            (k, v) -> {
              varArgRexNodeList.add(
                  context.rexBuilder.makeLiteral(
                      k,
                      context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                      true));
              varArgRexNodeList.add(
                  context.rexBuilder.makeLiteral(
                      v,
                      context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
                      true));
            });
    return context.rexBuilder.makeCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, varArgRexNodeList);
  }

  @Override
  public RexNode visitUnresolvedArgument(UnresolvedArgument node, CalcitePlanContext context) {
    RexNode value = analyze(node.getValue(), context);
    /*
     * Calcite SqlStdOperatorTable.AS doesn't have implementor registration in RexImpTable.
     * To not block ReduceExpressionsRule constants reduction optimization, use MAP_VALUE_CONSTRUCTOR instead to achieve the same effect.
     */
    return context.rexBuilder.makeCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
        context.rexBuilder.makeLiteral(node.getArgName()),
        value);
  }
}
