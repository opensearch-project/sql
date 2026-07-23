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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.ForeachPlaceholder;
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
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.Sort.SortOrder;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext.ForeachBinding;
import org.opensearch.sql.calcite.CalcitePlanContext.ForeachBindingType;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.calcite.utils.SubsearchUtils;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.CoercionUtils;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

@RequiredArgsConstructor
public class CalciteRexNodeVisitor extends AbstractNodeVisitor<RexNode, CalcitePlanContext> {
  private static final Pattern FOREACH_TEMPLATE = Pattern.compile("<<([A-Za-z0-9_]+)>>");

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
  public RexNode visitAggregateFunction(AggregateFunction node, CalcitePlanContext context) {
    // Resolve post-aggregate AggregateFunction via registry populated in visitAggregation.
    Integer index = context.getAggregateOutputIndex().get(node);
    if (index == null) {
      throw new IllegalStateException(
          "Aggregate function " + node + " not registered (planner bug)");
    }
    return context.relBuilder.field(index);
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
        RexNode foreachTemplate = foreachTemplateLiteral(value.toString(), context);
        if (foreachTemplate != null) {
          return foreachTemplate;
        }
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
    // When the field is a temporal type, do NOT use leastRestrictive + rexBuilder.makeIn. For a
    // temporal field tested against string/date literals, leastRestrictive collapses the common
    // type to VARCHAR (the EXPR_DATE / EXPR_TIMESTAMP UDTs are VARCHAR-backed), so makeIn casts the
    // field DOWN to VARCHAR and string-compares mismatched renderings (e.g. '2018-06-23 00:00:00'
    // against '2018-06-23') — silently matching nothing. Rewrite the membership test as an OR of
    // PPL `=` comparisons, the same temporal-aware comparison path visitCompare takes for `=`, so
    // each value is coerced to the field's timestamp domain before comparison.
    ExprType fieldExprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
    if (TEMPORAL_TYPES.contains(fieldExprType)) {
      List<RexNode> equalities =
          valueList.stream()
              .map(value -> PPLFuncImpTable.INSTANCE.resolve(context.rexBuilder, "=", field, value))
              .toList();
      return context.relBuilder.or(equalities);
    }
    final List<RelDataType> dataTypes =
        new ArrayList<>(valueList.stream().map(RexNode::getType).toList());
    dataTypes.add(field.getType());
    RelDataType commonType = context.rexBuilder.getTypeFactory().leastRestrictive(dataTypes);
    if (commonType != null) {
      List<RexNode> newValueList =
          valueList.stream().map(value -> context.rexBuilder.makeCast(commonType, value)).toList();
      return context.rexBuilder.makeIn(field, newValueList);
    }
    List<ExprType> exprTypes =
        dataTypes.stream().map(OpenSearchTypeFactory::convertRelDataTypeToExprType).toList();
    throw new SemanticCheckException(
        StringUtils.format(
            "In expression types are incompatible: fields type %s, values type %s",
            exprTypes.getLast(), exprTypes.subList(0, exprTypes.size() - 1)));
  }

  private static final Set<ExprType> TEMPORAL_TYPES =
      Set.of(ExprCoreType.DATE, ExprCoreType.TIME, ExprCoreType.TIMESTAMP);

  @Override
  public RexNode visitCompare(Compare node, CalcitePlanContext context) {
    RexNode left = analyze(node.getLeft(), context);
    RexNode right = analyze(node.getRight(), context);
    String op = node.getOperator();
    // Handle boolean_field != literal -> IS_NOT_TRUE/IS_NOT_FALSE
    if ("!=".equals(op) || "<>".equals(op)) {
      RexNode result = tryMakeBooleanNotEquals(left, right, context);
      if (result != null) {
        return result;
      }
    }
    return PPLFuncImpTable.INSTANCE.resolve(context.rexBuilder, op, left, right);
  }

  /**
   * Widens a set of operands to a common temporal type when, and only when, every operand is a
   * temporal type (DATE / TIME / TIMESTAMP), including the EXPR_DATE / EXPR_TIME / EXPR_TIMESTAMP
   * UDTs. Returns {@code null} otherwise so non-temporal incompatible mixes still fail the type
   * check. The widening reuses {@link CoercionUtils#widenArguments} — the same path comparison
   * operators take — which resolves DATE / TIME to TIMESTAMP via the shared widening graph.
   */
  private static @Nullable List<RexNode> widenTemporalOperands(
      CalcitePlanContext context, List<RexNode> operands) {
    boolean allTemporal =
        operands.stream()
            .map(node -> OpenSearchTypeFactory.convertRelDataTypeToExprType(node.getType()))
            .allMatch(TEMPORAL_TYPES::contains);
    if (!allTemporal) {
      return null;
    }
    return CoercionUtils.widenArguments(context.rexBuilder, operands);
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
      // leastRestrictive() has no common type for mixed temporal representations — e.g. a standard
      // Calcite TIMESTAMP field compared against EXPR_DATE UDT bounds (`ts between date('...') and
      // date('...')`). Comparison operators coerce these through CoercionUtils; BETWEEN calls
      // leastRestrictive directly and would otherwise reject them. Fall back to the same temporal
      // widening, scoped to all-temporal operands so genuinely incompatible mixes (e.g.
      // `age between '35' and 38.5`) still raise SemanticCheckException.
      List<RexNode> widened =
          widenTemporalOperands(context, List.of(value, lowerBound, upperBound));
      if (widened == null) {
        throw new SemanticCheckException(
            StringUtils.format(
                "BETWEEN expression types are incompatible: [%s, %s, %s]",
                OpenSearchTypeFactory.convertRelDataTypeToExprType(value.getType()),
                OpenSearchTypeFactory.convertRelDataTypeToExprType(lowerBound.getType()),
                OpenSearchTypeFactory.convertRelDataTypeToExprType(upperBound.getType())));
      }
      value = widened.get(0);
      lowerBound = widened.get(1);
      upperBound = widened.get(2);
    }
    return context.relBuilder.between(value, lowerBound, upperBound);
  }

  @Override
  public RexNode visitEqualTo(EqualTo node, CalcitePlanContext context) {
    final RexNode left = analyze(node.getLeft(), context);
    final RexNode right = analyze(node.getRight(), context);
    return context.rexBuilder.equals(left, right);
  }

  // ==================== Boolean NOT comparison helpers ====================
  // Calcite's RexSimplify transforms:
  // - "field = true" -> "field" (handled by PredicateAnalyzer detecting boolean field)
  // - "field = false" -> "NOT(field)" (handled by PredicateAnalyzer.prefix())
  // - "NOT(field = true)" -> "NOT(field)" -> would generate term{false}, have conflicted semantics
  // - "NOT(field = false)" -> "NOT(NOT(field))" -> "field" -> would generate term{true}, have
  // conflicted semantics
  // We intercept NOT(field = true/false) at AST level before Calcite optimization:
  // - "NOT(field = true)" -> IS_NOT_TRUE(field): matches false, null, missing
  // - "NOT(field = false)" -> IS_NOT_FALSE(field): matches true, null, missing

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
    // Only match actual field references, not arbitrary boolean expressions like CASE
    return node instanceof RexInputRef && node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
  }

  private boolean isBooleanLiteral(RexNode node) {
    return node instanceof RexLiteral && node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
  }

  /** Resolve qualified name. Note, the name should be case-sensitive. */
  @Override
  public RexNode visitQualifiedName(QualifiedName node, CalcitePlanContext context) {
    String name = node.toString();
    RexNode computed = context.getForeachComputedBindings().get(name.toUpperCase(Locale.ROOT));
    if (computed != null) {
      return computed;
    }
    ForeachBinding binding =
        context.getForeachIdentifierBindings().get(name.toUpperCase(Locale.ROOT));
    if (binding != null) {
      return foreachBindingToRexNode(node.toString(), binding, context);
    }
    return QualifiedNameResolver.resolve(node, context);
  }

  @Override
  public RexNode visitForeachPlaceholder(ForeachPlaceholder node, CalcitePlanContext context) {
    ForeachBinding binding = foreachBinding(node.getName(), context);
    if (binding == null) {
      throw new SemanticCheckException("Unresolved foreach placeholder <<" + node.getName() + ">>");
    }
    return foreachBindingToRexNode(node.getName(), binding, context);
  }

  private ForeachBinding foreachBinding(String name, CalcitePlanContext context) {
    return context.getForeachBindings().get(name.toUpperCase(Locale.ROOT));
  }

  private RexNode foreachTemplateLiteral(String value, CalcitePlanContext context) {
    Matcher matcher = FOREACH_TEMPLATE.matcher(value);
    List<RexNode> parts = new ArrayList<>();
    int start = 0;
    boolean replaced = false;
    while (matcher.find()) {
      ForeachBinding binding = foreachBinding(matcher.group(1), context);
      if (binding == null) {
        continue;
      }
      if (matcher.start() > start) {
        parts.add(context.rexBuilder.makeLiteral(value.substring(start, matcher.start())));
      }
      if (binding.type() == ForeachBindingType.PAIR_SLOT) {
        RelDataType varchar =
            context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        parts.add(
            context.rexBuilder.makeCast(
                varchar, foreachBindingToRexNode(matcher.group(1), binding, context), true, true));
      } else {
        parts.add(context.rexBuilder.makeLiteral(binding.value()));
      }
      start = matcher.end();
      replaced = true;
    }
    if (!replaced) {
      return null;
    }
    if (start < value.length()) {
      parts.add(context.rexBuilder.makeLiteral(value.substring(start)));
    }
    if (parts.isEmpty()) {
      return context.rexBuilder.makeLiteral("");
    }
    RexNode result = parts.getFirst();
    for (int i = 1; i < parts.size(); i++) {
      result = context.rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, result, parts.get(i));
    }
    return result;
  }

  private RexNode foreachBindingToRexNode(
      String name, ForeachBinding binding, CalcitePlanContext context) {
    switch (binding.type()) {
      case FIELD:
        return context.relBuilder.field(binding.value());
      case PAIR_SLOT:
        RexLambdaRef pair = context.getRexLambdaRefMap().get(binding.value());
        if (pair == null) {
          throw new SemanticCheckException("Unresolved foreach lambda placeholder " + name);
        }
        // The slot's type is known at plan time, so assign it directly to the opaque extraction
        // call. LambdaUtils' re-inference preserves it (a CAST would not survive the enumerable
        // backend for complex types like arrays).
        return context.rexBuilder.makeCall(
            binding.pairType(),
            PPLBuiltinOperators.FOREACH_PAIR_ITEM,
            List.of(
                pair,
                context.rexBuilder.makeExactLiteral(BigDecimal.valueOf(binding.pairIndex()))));
      case LITERAL:
      default:
        return context.rexBuilder.makeLiteral(binding.value());
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
    if (node.getConcatPrefix() != null) {

      expr =
          context.rexBuilder.makeCall(
              SqlStdOperatorTable.CONCAT,
              context.rexBuilder.makeLiteral(
                  node.getConcatPrefix().getValue(),
                  context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                  true),
              expr);
    }
    if (node.getConcatSuffix() != null) {

      expr =
          context.rexBuilder.makeCall(
              SqlStdOperatorTable.CONCAT,
              expr,
              context.rexBuilder.makeLiteral(
                  node.getConcatSuffix().getValue(),
                  context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                  true));
    }
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
    // Resolve a group-by expression to its group-key output index.
    Integer groupKeyIndex = context.getGroupKeyOutputIndex().get(node);
    if (groupKeyIndex != null) {
      return context.relBuilder.field(groupKeyIndex);
    }
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
    // SQL emits AggregateFunction for aggregate-as-window (e.g., SUM(x) OVER); PPL emits Function.
    final String funcName;
    final List<RexNode> arguments;
    final boolean isDistinct;
    if (node.getFunction() instanceof AggregateFunction aggFunc) {
      funcName = aggFunc.getFuncName();
      isDistinct = Boolean.TRUE.equals(aggFunc.getDistinct());
      List<UnresolvedExpression> argExprs = new ArrayList<>();
      if (aggFunc.getField() != null) {
        argExprs.add(aggFunc.getField());
      }
      argExprs.addAll(aggFunc.getArgList());
      arguments = argExprs.stream().map(arg -> analyze(arg, context)).toList();
    } else {
      Function windowFunction = (Function) node.getFunction();
      funcName = windowFunction.getFuncName();
      isDistinct = false;
      arguments = windowFunction.getFuncArgs().stream().map(arg -> analyze(arg, context)).toList();
    }
    List<RexNode> partitions =
        node.getPartitionByList().stream()
            .map(arg -> analyze(arg, context))
            .map(this::extractRexNodeFromAlias)
            .toList();
    List<RexNode> orderKeys = translateOrderKeys(node.getSortList(), context);
    return BuiltinFunctionName.ofWindowFunction(funcName)
        .map(
            functionName -> {
              RexNode field = arguments.isEmpty() ? null : arguments.getFirst();
              List<RexNode> args =
                  (arguments.isEmpty() || arguments.size() == 1)
                      ? Collections.emptyList()
                      : arguments.subList(1, arguments.size());
              // ROW_NUMBER takes no field/args and isn't in aggFunctionRegistry,
              // so skip aggregate signature validation.
              if (functionName == BuiltinFunctionName.ROW_NUMBER) {
                return PlanUtils.makeOver(
                    context,
                    functionName,
                    field,
                    args,
                    partitions,
                    orderKeys,
                    node.getWindowFrame());
              }
              List<RexNode> nodes =
                  PPLFuncImpTable.INSTANCE.validateAggFunctionSignature(
                      functionName, field, args, context.rexBuilder);
              return nodes != null
                  ? PlanUtils.makeOver(
                      context,
                      functionName,
                      isDistinct,
                      nodes.getFirst(),
                      nodes.size() <= 1 ? Collections.emptyList() : nodes.subList(1, nodes.size()),
                      partitions,
                      orderKeys,
                      node.getWindowFrame())
                  : PlanUtils.makeOver(
                      context,
                      functionName,
                      isDistinct,
                      field,
                      args,
                      partitions,
                      orderKeys,
                      node.getWindowFrame());
            })
        .orElseThrow(
            () -> new CalciteUnsupportedException("Unexpected window function: " + funcName));
  }

  private List<RexNode> translateOrderKeys(
      List<Pair<SortOption, UnresolvedExpression>> sortList, CalcitePlanContext context) {
    RelBuilder b = context.relBuilder;
    return sortList.stream()
        .map(
            p -> {
              SortOption opt = p.getLeft();
              RexNode field = analyze(p.getRight(), context);
              if (opt.getSortOrder() == SortOrder.DESC) {
                field = b.desc(field);
              }
              return switch (opt.getNullOrder()) {
                // Unspecified NULLS defaults to NULLS FIRST, matching top-level ORDER BY.
                case null -> b.nullsFirst(field);
                case NULL_FIRST -> b.nullsFirst(field);
                case NULL_LAST -> b.nullsLast(field);
              };
            })
        .toList();
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
    List<RelDataType> resultTypes = new ArrayList<>();
    for (When when : node.getWhenClauses()) {
      RexNode condition = analyze(when.getCondition(), context);
      if (!SqlTypeUtil.isBoolean(condition.getType())) {
        throw new ExpressionEvaluationException(
            StringUtils.format(
                "Condition expected a boolean type, but got %s", condition.getType()));
      }
      caseOperands.add(condition);
      RexNode result = analyze(when.getResult(), context);
      caseOperands.add(result);
      resultTypes.add(result.getType());
    }
    RexNode elseExpr =
        node.getElseClause().map(e -> analyze(e, context)).orElse(context.relBuilder.literal(null));
    caseOperands.add(elseExpr);
    resultTypes.add(elseExpr.getType());

    // Pre-validate the THEN/ELSE result types so an unsupertyped mix surfaces as a clean
    // 400 here instead of an opaque NPE deep in Calcite's makeCall return-type inference.
    RelDataType commonType = context.rexBuilder.getTypeFactory().leastRestrictive(resultTypes);
    if (commonType == null) {
      throw new ExpressionEvaluationException(
          StringUtils.format("case branches must have a common type, but got %s", resultTypes));
    }
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
