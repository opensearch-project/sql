/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.SqlKind.AS;
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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
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
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.ScoreFunction;
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
import org.opensearch.sql.ast.expression.subquery.SubqueryExpression;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.calcite.utils.SubsearchUtils;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
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
    // Special handling for NOT(boolean_field = true/false) - see boolean comparison helpers below
    UnresolvedExpression inner = node.getExpression();
    if (inner instanceof Compare compare && "=".equals(compare.getOperator())) {
      RexNode result = tryMakeBooleanNotEquals(compare, context);
      if (result != null) {
        return result;
      }
    }
    return context.relBuilder.not(analyze(node.getExpression(), context));
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
    String op = node.getOperator();
    if ("=".equals(op)) {
      RexNode result = tryMakeBooleanEquals(left, right, context);
      if (result != null) {
        return result;
      }
    } else if ("!=".equals(op) || "<>".equals(op)) {
      RexNode result = tryMakeBooleanNotEquals(left, right, context);
      if (result != null) {
        return result;
      }
    }
    return PPLFuncImpTable.INSTANCE.resolve(context.rexBuilder, op, left, right);
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
    RexNode left = analyze(node.getLeft(), context);
    RexNode right = analyze(node.getRight(), context);
    RexNode result = tryMakeBooleanEquals(left, right, context);
    return result != null ? result : context.rexBuilder.equals(left, right);
  }

  // ==================== Boolean comparison helpers ====================
  // Calcite's RexSimplify transforms "field = true" to "field" and "field = false" to "NOT(field)".
  // This loses null-handling semantics in OpenSearch. We intercept these patterns at AST level:
  // - "field = true" -> IS_TRUE(field): only matches documents where field is explicitly true
  // - "field = false" -> normal EQUALS: PredicateAnalyzer handles NOT(field) pattern
  // - "NOT(field = true)" -> IS_NOT_TRUE(field): matches false, null, missing
  // - "NOT(field = false)" -> IS_NOT_FALSE(field): matches true, null, missing

  /**
   * Try to convert boolean field equality to IS_TRUE/IS_FALSE. This prevents Calcite's RexSimplify
   * from transforming these expressions and losing the exact match semantics.
   */
  private RexNode tryMakeBooleanEquals(RexNode left, RexNode right, CalcitePlanContext context) {
    BooleanFieldComparison cmp = extractBooleanFieldComparison(left, right);
    if (cmp == null) {
      return null;
    }
    SqlOperator op =
        Boolean.TRUE.equals(cmp.literalValue)
            ? SqlStdOperatorTable.IS_TRUE
            : SqlStdOperatorTable.IS_FALSE;
    return context.rexBuilder.makeCall(op, cmp.field);
  }

  /**
   * Try to convert boolean_field != literal or NOT(boolean_field = literal) to
   * IS_NOT_TRUE/IS_NOT_FALSE. This preserves correct null-handling semantics.
   */
  private RexNode tryMakeBooleanNotEquals(RexNode left, RexNode right, CalcitePlanContext context) {
    BooleanFieldComparison cmp = extractBooleanFieldComparison(left, right);
    if (cmp == null) {
      return null;
    }
    SqlOperator op =
        Boolean.FALSE.equals(cmp.literalValue)
            ? SqlStdOperatorTable.IS_NOT_FALSE
            : SqlStdOperatorTable.IS_NOT_TRUE;
    return context.rexBuilder.makeCall(op, cmp.field);
  }

  /** Overload for NOT(Compare) AST pattern. */
  private RexNode tryMakeBooleanNotEquals(Compare compare, CalcitePlanContext context) {
    return tryMakeBooleanNotEquals(
        analyze(compare.getLeft(), context), analyze(compare.getRight(), context), context);
  }

  /** Represents a comparison between a boolean field and a boolean literal. */
  private record BooleanFieldComparison(RexNode field, Boolean literalValue) {}

  /**
   * Extract boolean field and literal value from a comparison, normalizing operand order. Returns
   * null if the comparison is not between a boolean field and a boolean literal.
   */
  private BooleanFieldComparison extractBooleanFieldComparison(RexNode left, RexNode right) {
    if (isBooleanField(left) && isBooleanLiteral(right)) {
      return new BooleanFieldComparison(left, ((RexLiteral) right).getValueAs(Boolean.class));
    }
    if (isBooleanField(right) && isBooleanLiteral(left)) {
      return new BooleanFieldComparison(right, ((RexLiteral) left).getValueAs(Boolean.class));
    }
    return null;
  }

  private boolean isBooleanField(RexNode node) {
    return node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN && !(node instanceof RexLiteral);
  }

  private boolean isBooleanLiteral(RexNode node) {
    return node instanceof RexLiteral && node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
  }

  /** Resolve qualified name. Note, the name should be case-sensitive. */
  @Override
  public RexNode visitQualifiedName(QualifiedName node, CalcitePlanContext context) {
    return QualifiedNameResolver.resolve(node, context);
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

      // Add captured variables as additional lambda parameters
      // They are stored with keys like "__captured_0", "__captured_1", etc.
      List<RexNode> capturedVars = context.getCapturedVariables();
      if (capturedVars != null && !capturedVars.isEmpty()) {
        args = new ArrayList<>(args);
        for (int i = 0; i < capturedVars.size(); i++) {
          RexLambdaRef capturedRef = context.getRexLambdaRefMap().get("__captured_" + i);
          if (capturedRef != null) {
            args.add(capturedRef);
          }
        }
      }

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

    boolean isCoalesce = "coalesce".equalsIgnoreCase(node.getFuncName());
    if (isCoalesce) {
      context.setInCoalesceFunction(true);
    }

    List<RexNode> capturedVars = null;
    try {
      for (UnresolvedExpression arg : args) {
        if (arg instanceof LambdaFunction) {
          CalcitePlanContext lambdaContext =
              prepareLambdaContext(
                  context, (LambdaFunction) arg, arguments, node.getFuncName(), null);
          RexNode lambdaNode = analyze(arg, lambdaContext);
          if (node.getFuncName().equalsIgnoreCase("reduce")) {
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
          // Capture any external variables that were referenced in the lambda
          capturedVars = lambdaContext.getCapturedVariables();
        } else {
          arguments.add(analyze(arg, context));
        }
      }
    } finally {
      if (isCoalesce) {
        context.setInCoalesceFunction(false);
      }
    }

    // For transform/mvmap functions with captured variables, add them as additional arguments
    if (capturedVars != null && !capturedVars.isEmpty()) {
      if (node.getFuncName().equalsIgnoreCase("mvmap")
          || node.getFuncName().equalsIgnoreCase("transform")) {
        arguments = new ArrayList<>(arguments);
        arguments.addAll(capturedVars);
      }
    }

    if ("LIKE".equalsIgnoreCase(node.getFuncName()) && arguments.size() == 2) {
      RexNode defaultCaseSensitive =
          CalcitePlanContext.isLegacyPreferred()
              ? context.rexBuilder.makeLiteral(false)
              : context.rexBuilder.makeLiteral(true);
      arguments = new ArrayList<>(arguments);
      arguments.add(defaultCaseSensitive);
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
              List<RexNode> nodes =
                  PPLFuncImpTable.INSTANCE.validateAggFunctionSignature(
                      functionName, field, args, context.rexBuilder);
              return nodes != null
                  ? PlanUtils.makeOver(
                      context,
                      functionName,
                      nodes.getFirst(),
                      nodes.size() <= 1 ? Collections.emptyList() : nodes.subList(1, nodes.size()),
                      partitions,
                      List.of(),
                      node.getWindowFrame())
                  : PlanUtils.makeOver(
                      context,
                      functionName,
                      field,
                      args,
                      partitions,
                      List.of(),
                      node.getWindowFrame());
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
    RelNode subqueryRel = resolveSubqueryPlan(subquery, node, context);
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
          return resolveSubqueryPlan(subquery, node, context);
        });
  }

  @Override
  public RexNode visitExistsSubquery(ExistsSubquery node, CalcitePlanContext context) {
    return context.relBuilder.exists(
        b -> {
          UnresolvedPlan subquery = node.getQuery();
          return resolveSubqueryPlan(subquery, node, context);
        });
  }

  private RelNode resolveSubqueryPlan(
      UnresolvedPlan subquery, SubqueryExpression subqueryExpression, CalcitePlanContext context) {
    boolean isNestedSubquery = context.isResolvingSubquery();
    context.setResolvingSubquery(true);
    // clear and store the outer state
    boolean isResolvingJoinConditionOuter = context.isResolvingJoinCondition();
    if (isResolvingJoinConditionOuter) {
      context.setResolvingJoinCondition(false);
    }
    subquery.accept(planVisitor, context);
    // add subsearch.maxout limit to exists-in subsearch, 0 and negative means unlimited
    if (context.sysLimit.subsearchLimit() > 0 && !(subqueryExpression instanceof ScalarSubquery)) {
      // Cannot add system limit to the top of subquery simply.
      // Instead, add system limit under the correlated conditions.
      SubsearchUtils.SystemLimitInsertionShuttle shuttle =
          new SubsearchUtils.SystemLimitInsertionShuttle(context);
      RelNode replacement = context.relBuilder.peek().accept(shuttle);
      if (!shuttle.isCorrelatedConditionFound()) {
        // If no correlated condition found, add system limit to the top of subquery.
        replacement =
            LogicalSystemLimit.create(
                SystemLimitType.SUBSEARCH_MAXOUT,
                replacement,
                context.relBuilder.literal(context.sysLimit.subsearchLimit()));
      }
      PlanUtils.replaceTop(context.relBuilder, replacement);
    }
    // pop the inner plan
    RelNode subqueryRel = context.relBuilder.build();
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
      RexNode condition = analyze(when.getCondition(), context);
      if (!SqlTypeUtil.isBoolean(condition.getType())) {
        throw new ExpressionEvaluationException(
            StringUtils.format(
                "Condition expected a boolean type, but got %s", condition.getType()));
      }
      caseOperands.add(condition);
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
  public RexNode visitHighlightFunction(HighlightFunction node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Highlight function is unsupported in Calcite");
  }

  @Override
  public RexNode visitScoreFunction(ScoreFunction node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Score function is unsupported in Calcite");
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
