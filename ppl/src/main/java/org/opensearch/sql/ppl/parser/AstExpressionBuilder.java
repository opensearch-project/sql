/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.*;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BinaryArithmeticContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BySpanClauseContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.CompareExprContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.ConvertedDataTypeContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.CountAllFunctionCallContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.CountEvalFunctionCallContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DataTypeFunctionCallContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DecimalLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DistinctCountFunctionCallContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DoubleLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalClauseContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalExpressionContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalFunctionCallContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldExpressionContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FloatLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IdentsAsQualifiedNameContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IdentsAsTableQualifiedNameContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IdentsAsWildcardQualifiedNameContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.InExprContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntegerLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntervalLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalAndContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalNotContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalOrContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalXorContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.MultiFieldRelevanceFunctionContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RenameFieldExpressionContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SingleFieldRelevanceFunctionContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortFieldContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SpanClauseContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsFunctionCallContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StringLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TableSourceContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.WcFieldExpressionContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ppl.utils.ArgumentFactory;

/** Class of building AST Expression nodes. */
public class AstExpressionBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedExpression> {

  private static final int DEFAULT_TAKE_FUNCTION_SIZE_VALUE = 10;

  /** The function name mapping between fronted and core engine. */
  private static final Map<String, String> FUNCTION_NAME_MAPPING =
      new ImmutableMap.Builder<String, String>()
          .put("isnull", IS_NULL.getName().getFunctionName())
          .put("isnotnull", IS_NOT_NULL.getName().getFunctionName())
          .build();

  private final AstBuilder astBuilder;

  public AstExpressionBuilder(AstBuilder astBuilder) {
    this.astBuilder = astBuilder;
  }

  /** Eval clause. */
  @Override
  public UnresolvedExpression visitEvalClause(EvalClauseContext ctx) {
    return new Let((Field) visit(ctx.fieldExpression()), visit(ctx.logicalExpression()));
  }

  /** Trendline clause. */
  @Override
  public Trendline.TrendlineComputation visitTrendlineClause(
      OpenSearchPPLParser.TrendlineClauseContext ctx) {
    final int numberOfDataPoints = Integer.parseInt(ctx.numberOfDataPoints.getText());
    if (numberOfDataPoints < 1) {
      throw new SyntaxCheckException(
          "Number of trendline data-points must be greater than or equal to 1");
    }

    final Field dataField = (Field) this.visitFieldExpression(ctx.field);
    final String alias =
        ctx.alias != null
            ? ctx.alias.getText()
            : dataField.getChild().get(0).toString() + "_trendline";

    final Trendline.TrendlineType computationType =
        Trendline.TrendlineType.valueOf(ctx.trendlineType().getText().toUpperCase(Locale.ROOT));
    return new Trendline.TrendlineComputation(
        numberOfDataPoints, dataField, alias, computationType);
  }

  /** Logical expression excluding boolean, comparison. */
  @Override
  public UnresolvedExpression visitLogicalNot(LogicalNotContext ctx) {
    return new Not(visit(ctx.logicalExpression()));
  }

  @Override
  public UnresolvedExpression visitLogicalOr(LogicalOrContext ctx) {
    return new Or(visit(ctx.left), visit(ctx.right));
  }

  @Override
  public UnresolvedExpression visitLogicalAnd(LogicalAndContext ctx) {
    return new And(visit(ctx.left), visit(ctx.right));
  }

  @Override
  public UnresolvedExpression visitLogicalXor(LogicalXorContext ctx) {
    return new Xor(visit(ctx.left), visit(ctx.right));
  }

  /** lambda expression */
  @Override
  public UnresolvedExpression visitLambda(OpenSearchPPLParser.LambdaContext ctx) {
    List<QualifiedName> arguments =
        ctx.ident().stream()
            .map(x -> this.visitIdentifiers(Collections.singletonList(x)))
            .collect(Collectors.toList());
    UnresolvedExpression function = visit(ctx.logicalExpression());
    return new LambdaFunction(function, arguments);
  }

  /** Comparison expression. */
  @Override
  public UnresolvedExpression visitCompareExpr(CompareExprContext ctx) {
    String operator = ctx.comparisonOperator().getText();
    if ("==".equals(operator)) {
      operator = EQUAL.getName().getFunctionName();
    } else if (LIKE.getName().getFunctionName().equalsIgnoreCase(operator)) {
      operator = LIKE.getName().getFunctionName();
    }
    return new Compare(operator, visit(ctx.left), visit(ctx.right));
  }

  @Override
  public UnresolvedExpression visitInExpr(InExprContext ctx) {
    UnresolvedExpression expr =
        new In(
            visit(ctx.expression()),
            ctx.valueList().literalValue().stream()
                .map(this::visitLiteralValue)
                .collect(Collectors.toList()));
    return ctx.NOT() != null ? new Not(expr) : expr;
  }

  /** Value Expression. */
  @Override
  public UnresolvedExpression visitBinaryArithmetic(BinaryArithmeticContext ctx) {
    return new Function(ctx.binaryOperator.getText(), buildArguments(ctx.left, ctx.right));
  }

  private List<UnresolvedExpression> buildArguments(
      OpenSearchPPLParser.ValueExpressionContext... ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    for (OpenSearchPPLParser.ValueExpressionContext value : ctx) {
      UnresolvedExpression unresolvedExpression = visit(value);
      if (unresolvedExpression != null) builder.add(unresolvedExpression);
    }
    return builder.build();
  }

  @Override
  public UnresolvedExpression visitNestedValueExpr(OpenSearchPPLParser.NestedValueExprContext ctx) {
    return visit(ctx.logicalExpression()); // Discard parenthesis around
  }

  /** Field expression. */
  @Override
  public UnresolvedExpression visitFieldExpression(FieldExpressionContext ctx) {
    return new Field((QualifiedName) visit(ctx.qualifiedName()));
  }

  @Override
  public UnresolvedExpression visitWcFieldExpression(WcFieldExpressionContext ctx) {
    return new Field((QualifiedName) visit(ctx.wcQualifiedName()));
  }

  @Override
  public UnresolvedExpression visitSelectFieldExpression(
      OpenSearchPPLParser.SelectFieldExpressionContext ctx) {
    if (ctx.STAR() != null) {
      return AllFields.of();
    }
    return new Field((QualifiedName) visit(ctx.wcQualifiedName()));
  }

  @Override
  public UnresolvedExpression visitRenameFieldExpression(RenameFieldExpressionContext ctx) {
    if (ctx.STAR() != null) {
      return new Field(QualifiedName.of("*"));
    }
    return new Field((QualifiedName) visit(ctx.wcQualifiedName()));
  }

  @Override
  public UnresolvedExpression visitSortField(SortFieldContext ctx) {

    UnresolvedExpression fieldExpression =
        visit(ctx.sortFieldExpression().fieldExpression().qualifiedName());

    if (ctx.sortFieldExpression().IP() != null) {
      fieldExpression = new Cast(fieldExpression, AstDSL.stringLiteral("ip"));
    } else if (ctx.sortFieldExpression().NUM() != null) {
      fieldExpression = new Cast(fieldExpression, AstDSL.stringLiteral("double"));
    } else if (ctx.sortFieldExpression().STR() != null) {
      fieldExpression = new Cast(fieldExpression, AstDSL.stringLiteral("string"));
    }
    // AUTO() case uses the field expression as-is
    return new Field(fieldExpression, ArgumentFactory.getArgumentList(ctx));
  }

  /** Aggregation function. */
  @Override
  public UnresolvedExpression visitStatsFunctionCall(StatsFunctionCallContext ctx) {
    return buildAggregateFunction(
        ctx.statsFunctionName().getText(), ctx.functionArgs().functionArg());
  }

  @Override
  public UnresolvedExpression visitValuesAggFunctionCall(
      OpenSearchPPLParser.ValuesAggFunctionCallContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();

    // Get limit from settings
    int limit = 0; // Default to unlimited
    if (astBuilder.getSettings() != null) {
      Integer settingValue =
          astBuilder
              .getSettings()
              .getSettingValue(org.opensearch.sql.common.setting.Settings.Key.PPL_VALUES_MAX_LIMIT);
      if (settingValue != null) {
        limit = settingValue;
      }
    }

    // Only add limit parameter if it's non-zero (i.e., explicitly configured)
    if (limit > 0) {
      builder.add(new UnresolvedArgument("limit", AstDSL.intLiteral(limit)));
    }

    return new AggregateFunction(
        "values", visit(ctx.valuesAggFunction().valueExpression()), builder.build());
  }

  private AggregateFunction buildAggregateFunction(
      String functionName, List<OpenSearchPPLParser.FunctionArgContext> args) {
    List<UnresolvedExpression> unresolvedArgs =
        args.stream().map(this::visitFunctionArg).collect(Collectors.toList());

    return new AggregateFunction(
        functionName, unresolvedArgs.get(0), unresolvedArgs.subList(1, unresolvedArgs.size()));
  }

  @Override
  public UnresolvedExpression visitCountAllFunctionCall(CountAllFunctionCallContext ctx) {
    return new AggregateFunction("count", AllFields.of());
  }

  @Override
  public UnresolvedExpression visitCountEvalFunctionCall(CountEvalFunctionCallContext ctx) {
    return new AggregateFunction("count", visit(ctx.evalExpression()));
  }

  @Override
  public UnresolvedExpression visitDistinctCountFunctionCall(DistinctCountFunctionCallContext ctx) {
    String funcName = ctx.DISTINCT_COUNT_APPROX() != null ? "distinct_count_approx" : "count";
    return new AggregateFunction(funcName, visit(ctx.valueExpression()), true);
  }

  @Override
  public UnresolvedExpression visitEvalExpression(EvalExpressionContext ctx) {
    /*
     * Rewrite "eval(p)" as "CASE WHEN p THEN 1 ELSE NULL END" so that COUNT or DISTINCT_COUNT
     * can correctly perform filtered counting.
     * Note: at present only eval(<predicate>) inside counting functions is supported.
     */
    UnresolvedExpression predicate = visit(ctx.logicalExpression());
    return AstDSL.caseWhen(null, AstDSL.when(predicate, AstDSL.intLiteral(1)));
  }

  @Override
  public UnresolvedExpression visitPercentileApproxFunctionCall(
      OpenSearchPPLParser.PercentileApproxFunctionCallContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("percent", visit(ctx.percentileApproxFunction().percent)));
    if (ctx.percentileApproxFunction().compression != null) {
      builder.add(
          new UnresolvedArgument("compression", visit(ctx.percentileApproxFunction().compression)));
    }
    return new AggregateFunction(
        "percentile", visit(ctx.percentileApproxFunction().aggField), builder.build());
  }

  @Override
  public UnresolvedExpression visitTakeAggFunctionCall(
      OpenSearchPPLParser.TakeAggFunctionCallContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(
        new UnresolvedArgument(
            "size",
            ctx.takeAggFunction().size != null
                ? visit(ctx.takeAggFunction().size)
                : AstDSL.intLiteral(DEFAULT_TAKE_FUNCTION_SIZE_VALUE)));
    return new AggregateFunction(
        "take", visit(ctx.takeAggFunction().fieldExpression()), builder.build());
  }

  @Override
  public UnresolvedExpression visitPercentileShortcutFunctionCall(
      OpenSearchPPLParser.PercentileShortcutFunctionCallContext ctx) {
    String functionName = ctx.getStart().getText();

    int prefixLength = functionName.toLowerCase().startsWith("perc") ? 4 : 1;
    String percentileValue = functionName.substring(prefixLength);

    double percent = Double.parseDouble(percentileValue);
    if (percent < 0.0 || percent > 100.0) {
      throw new SyntaxCheckException(
          String.format("Percentile value must be between 0 and 100, got: %s", percent));
    }

    return new AggregateFunction(
        "percentile",
        visit(ctx.valueExpression()),
        Collections.singletonList(
            new UnresolvedArgument("percent", AstDSL.doubleLiteral(percent))));
  }

  /** Case function. */
  @Override
  public UnresolvedExpression visitCaseFunctionCall(
      OpenSearchPPLParser.CaseFunctionCallContext ctx) {
    List<When> whens =
        IntStream.range(0, ctx.logicalExpression().size())
            .mapToObj(
                index -> {
                  UnresolvedExpression condition = visit(ctx.logicalExpression(index));
                  UnresolvedExpression result = visit(ctx.valueExpression(index));
                  return new When(condition, result);
                })
            .collect(Collectors.toList());
    UnresolvedExpression elseValue = null;
    if (ctx.ELSE() != null) {
      elseValue = visit(ctx.valueExpression(ctx.valueExpression().size() - 1));
    }
    return new Case(null, whens, Optional.ofNullable(elseValue));
  }

  /** Eval function. */
  @Override
  public UnresolvedExpression visitEvalFunctionCall(EvalFunctionCallContext ctx) {
    final String functionName = ctx.evalFunctionName().getText();
    final String mappedName =
        FUNCTION_NAME_MAPPING.getOrDefault(functionName.toLowerCase(Locale.ROOT), functionName);

    // Rewrite sum and avg functions to arithmetic expressions
    if (SUM.getName().getFunctionName().equalsIgnoreCase(mappedName)
        || AVG.getName().getFunctionName().equalsIgnoreCase(mappedName)) {
      return rewriteSumAvgFunction(mappedName, ctx.functionArgs().functionArg());
    }

    return buildFunction(mappedName, ctx.functionArgs().functionArg());
  }

  private Function buildFunction(
      String functionName, List<OpenSearchPPLParser.FunctionArgContext> args) {
    return new Function(
        functionName, args.stream().map(this::visitFunctionArg).collect(Collectors.toList()));
  }

  /** Cast function. */
  @Override
  public UnresolvedExpression visitDataTypeFunctionCall(DataTypeFunctionCallContext ctx) {
    return new Cast(visit(ctx.logicalExpression()), visit(ctx.convertedDataType()));
  }

  @Override
  public UnresolvedExpression visitConvertedDataType(ConvertedDataTypeContext ctx) {
    return AstDSL.stringLiteral(ctx.getText());
  }

  /**
   * Rewrites sum(a, b, c, ...) to (a + b + c + ...) and avg(a, b, c, ...) to (a + b + c + ...) / n
   * Uses balanced tree construction to avoid deep recursion with large argument lists.
   */
  private UnresolvedExpression rewriteSumAvgFunction(
      String functionName, List<OpenSearchPPLParser.FunctionArgContext> args) {
    if (args.isEmpty()) {
      throw new SyntaxCheckException(functionName + " function requires at least one argument");
    }

    List<UnresolvedExpression> arguments =
        args.stream().map(this::visitFunctionArg).collect(Collectors.toList());

    // Build the sum expression as a balanced tree to avoid deep recursion
    UnresolvedExpression functionExpr = buildBalancedTree("+", arguments);

    // For avg, divide by the count of arguments
    if (AVG.getName().getFunctionName().equalsIgnoreCase(functionName)) {
      UnresolvedExpression count = AstDSL.doubleLiteral((double) arguments.size());
      functionExpr = new Function("/", Arrays.asList(functionExpr, count));
    }

    return functionExpr;
  }

  /**
   * Builds a balanced tree of binary operations to avoid deep recursion. For example, [a, b, c, d]
   * becomes ((a + b) + (c + d)) instead of (((a + b) + c) + d). This ensures recursion depth is
   * O(log n) instead of O(n).
   */
  private UnresolvedExpression buildBalancedTree(
      String operator, List<UnresolvedExpression> expressions) {
    if (expressions.size() == 1) {
      return expressions.get(0);
    }

    if (expressions.size() == 2) {
      return new Function(operator, Arrays.asList(expressions.get(0), expressions.get(1)));
    }

    // Split the list in half and recursively build balanced subtrees
    int mid = expressions.size() / 2;
    UnresolvedExpression left = buildBalancedTree(operator, expressions.subList(0, mid));
    UnresolvedExpression right =
        buildBalancedTree(operator, expressions.subList(mid, expressions.size()));

    return new Function(operator, Arrays.asList(left, right));
  }

  @Override
  public UnresolvedExpression visitSingleFieldRelevanceFunction(
      SingleFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.singleFieldRelevanceFunctionName().getText().toLowerCase(Locale.ROOT),
        singleFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitMultiFieldRelevanceFunction(
      MultiFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(Locale.ROOT),
        multiFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitTableSource(TableSourceContext ctx) {
    if (ctx.getChild(0) instanceof IdentsAsTableQualifiedNameContext) {
      return visitIdentsAsTableQualifiedName((IdentsAsTableQualifiedNameContext) ctx.getChild(0));
    } else {
      return visitIdentifiers(Arrays.asList(ctx));
    }
  }

  @Override
  public UnresolvedExpression visitPositionFunctionCall(
      OpenSearchPPLParser.PositionFunctionCallContext ctx) {
    return new Function(
        POSITION.getName().getFunctionName(),
        Arrays.asList(visitFunctionArg(ctx.functionArg(0)), visitFunctionArg(ctx.functionArg(1))));
  }

  @Override
  public UnresolvedExpression visitExtractFunctionCall(
      OpenSearchPPLParser.ExtractFunctionCallContext ctx) {
    return new Function(ctx.EXTRACT().toString(), getExtractFunctionArguments(ctx));
  }

  private List<UnresolvedExpression> getExtractFunctionArguments(
      OpenSearchPPLParser.ExtractFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.datetimePart().getText(), DataType.STRING),
            visitFunctionArg(ctx.functionArg()));
    return args;
  }

  @Override
  public UnresolvedExpression visitGetFormatFunctionCall(
      OpenSearchPPLParser.GetFormatFunctionCallContext ctx) {
    return new Function(ctx.GET_FORMAT().toString(), getFormatFunctionArguments(ctx));
  }

  private List<UnresolvedExpression> getFormatFunctionArguments(
      OpenSearchPPLParser.GetFormatFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.getFormatType().getText(), DataType.STRING),
            visitFunctionArg(ctx.functionArg()));
    return args;
  }

  @Override
  public UnresolvedExpression visitTimestampFunctionCall(
      OpenSearchPPLParser.TimestampFunctionCallContext ctx) {
    return new Function(ctx.timestampFunctionName().getText(), timestampFunctionArguments(ctx));
  }

  private List<UnresolvedExpression> timestampFunctionArguments(
      OpenSearchPPLParser.TimestampFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.simpleDateTimePart().getText(), DataType.STRING),
            visitFunctionArg(ctx.firstArg),
            visitFunctionArg(ctx.secondArg));
    return args;
  }

  /** Literal and value. */
  @Override
  public UnresolvedExpression visitIdentsAsQualifiedName(IdentsAsQualifiedNameContext ctx) {
    return visitIdentifiers(ctx.ident());
  }

  @Override
  public UnresolvedExpression visitIdentsAsTableQualifiedName(
      IdentsAsTableQualifiedNameContext ctx) {
    return visitIdentifiers(
        Stream.concat(Stream.of(ctx.tableIdent()), ctx.ident().stream())
            .collect(Collectors.toList()));
  }

  @Override
  public UnresolvedExpression visitIdentsAsWildcardQualifiedName(
      IdentsAsWildcardQualifiedNameContext ctx) {
    return visitIdentifiers(ctx.wildcard());
  }

  @Override
  public UnresolvedExpression visitIntervalLiteral(IntervalLiteralContext ctx) {
    return new Interval(
        visit(ctx.valueExpression()), IntervalUnit.of(ctx.intervalUnit().getText()));
  }

  @Override
  public UnresolvedExpression visitStringLiteral(StringLiteralContext ctx) {
    return new Literal(StringUtils.unquoteText(ctx.getText()), DataType.STRING);
  }

  @Override
  public UnresolvedExpression visitIntegerLiteral(IntegerLiteralContext ctx) {
    long number = Long.parseLong(ctx.getText());
    if (Integer.MIN_VALUE <= number && number <= Integer.MAX_VALUE) {
      return new Literal((int) number, DataType.INTEGER);
    }
    return new Literal(number, DataType.LONG);
  }

  @Override
  public UnresolvedExpression visitDecimalLiteral(DecimalLiteralContext ctx) {
    return new Literal(new BigDecimal(ctx.getText()), DataType.DECIMAL);
  }

  @Override
  public UnresolvedExpression visitDoubleLiteral(DoubleLiteralContext ctx) {
    return new Literal(Double.valueOf(ctx.getText()), DataType.DOUBLE);
  }

  @Override
  public UnresolvedExpression visitFloatLiteral(FloatLiteralContext ctx) {
    return new Literal(Float.valueOf(ctx.getText()), DataType.FLOAT);
  }

  @Override
  public UnresolvedExpression visitBooleanLiteral(BooleanLiteralContext ctx) {
    return new Literal(Boolean.valueOf(ctx.getText()), DataType.BOOLEAN);
  }

  @Override
  public UnresolvedExpression visitBySpanClause(BySpanClauseContext ctx) {
    String name = ctx.spanClause().getText();
    return ctx.alias != null
        ? new Alias(StringUtils.unquoteIdentifier(ctx.alias.getText()), visit(ctx.spanClause()))
        : new Alias(name, visit(ctx.spanClause()));
  }

  @Override
  public UnresolvedExpression visitSpanClause(SpanClauseContext ctx) {
    String unit = ctx.unit != null ? ctx.unit.getText() : "";
    return new Span(visit(ctx.fieldExpression()), visit(ctx.value), SpanUnit.of(unit));
  }

  // Handle new syntax: span=1h
  @Override
  public UnresolvedExpression visitSpanLiteral(OpenSearchPPLParser.SpanLiteralContext ctx) {
    if (ctx.integerLiteral() != null && ctx.timespanUnit() != null) {
      return new Span(
          AstDSL.field("@timestamp"),
          new Literal(Integer.parseInt(ctx.integerLiteral().getText()), DataType.INTEGER),
          SpanUnit.of(ctx.timespanUnit().getText()));
    }

    if (ctx.integerLiteral() != null) {
      return new Span(
          AstDSL.field("@timestamp"),
          new Literal(Integer.parseInt(ctx.integerLiteral().getText()), DataType.INTEGER),
          SpanUnit.of(""));
    }

    return new Span(
        AstDSL.field("@timestamp"), new Literal(ctx.getText(), DataType.STRING), SpanUnit.of(""));
  }

  @Override
  public UnresolvedExpression visitLeftHint(OpenSearchPPLParser.LeftHintContext ctx) {
    return new EqualTo(
        new Literal(ctx.leftHintKey.getText(), DataType.STRING), visit(ctx.leftHintValue));
  }

  @Override
  public UnresolvedExpression visitRightHint(OpenSearchPPLParser.RightHintContext ctx) {
    return new EqualTo(
        new Literal(ctx.rightHintKey.getText(), DataType.STRING), visit(ctx.rightHintValue));
  }

  @Override
  public UnresolvedExpression visitInSubqueryExpr(OpenSearchPPLParser.InSubqueryExprContext ctx) {
    List<UnresolvedExpression> s =
        ctx.valueExpression().stream().map(this::visit).collect(Collectors.toList());
    UnresolvedExpression expr = new InSubquery(s, astBuilder.visitSubSearch(ctx.subSearch()));
    return ctx.NOT() != null ? new Not(expr) : expr;
  }

  @Override
  public UnresolvedExpression visitScalarSubqueryExpr(
      OpenSearchPPLParser.ScalarSubqueryExprContext ctx) {
    return new ScalarSubquery(astBuilder.visitSubSearch(ctx.subSearch()));
  }

  @Override
  public UnresolvedExpression visitExistsSubqueryExpr(
      OpenSearchPPLParser.ExistsSubqueryExprContext ctx) {
    return new ExistsSubquery(astBuilder.visitSubSearch(ctx.subSearch()));
  }

  @Override
  public UnresolvedExpression visitBetween(OpenSearchPPLParser.BetweenContext ctx) {
    UnresolvedExpression betweenExpr =
        new Between(visit(ctx.expression(0)), visit(ctx.expression(1)), visit(ctx.expression(2)));
    return ctx.NOT() != null ? new Not(betweenExpr) : betweenExpr;
  }

  @Override
  public UnresolvedExpression visitWindowFunction(OpenSearchPPLParser.WindowFunctionContext ctx) {
    Function f =
        buildFunction(ctx.windowFunctionName().getText(), ctx.functionArgs().functionArg());

    // In PPL eventstats command, all window functions have the same partition and order spec.
    return new WindowFunction(f);
  }

  @Override
  public UnresolvedExpression visitOverwriteOption(OpenSearchPPLParser.OverwriteOptionContext ctx) {
    return new Argument("overwrite", (Literal) this.visit(ctx.booleanLiteral()));
  }

  @Override
  public UnresolvedExpression visitJoinType(OpenSearchPPLParser.JoinTypeContext ctx) {
    return ArgumentFactory.getArgumentValue(ctx);
  }

  @Override
  public UnresolvedExpression visitMaxOption(OpenSearchPPLParser.MaxOptionContext ctx) {
    return new Argument("max", (Literal) this.visit(ctx.integerLiteral()));
  }

  private QualifiedName visitIdentifiers(List<? extends ParserRuleContext> ctx) {
    return new QualifiedName(
        ctx.stream()
            .map(RuleContext::getText)
            .map(StringUtils::unquoteIdentifier)
            .collect(Collectors.toList()));
  }

  private List<UnresolvedExpression> singleFieldRelevanceArguments(
      SingleFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(
        new UnresolvedArgument(
            "field", new QualifiedName(StringUtils.unquoteText(ctx.field.getText()))));
    builder.add(
        new UnresolvedArgument(
            "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    ctx.relevanceArg()
        .forEach(
            v ->
                builder.add(
                    new UnresolvedArgument(
                        v.relevanceArgName().getText().toLowerCase(Locale.ROOT),
                        new Literal(
                            StringUtils.unquoteText(v.relevanceArgValue().getText()),
                            DataType.STRING))));
    return builder.build();
  }

  private List<UnresolvedExpression> multiFieldRelevanceArguments(
      MultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();

    // Handle optional fields - only add fields argument if fields are present
    var fieldContexts =
        ctx.getRuleContexts(OpenSearchPPLParser.RelevanceFieldAndWeightContext.class);
    if (fieldContexts != null && !fieldContexts.isEmpty()) {
      var fields =
          new RelevanceFieldList(
              fieldContexts.stream()
                  .collect(
                      Collectors.toMap(
                          f -> StringUtils.unquoteText(f.field.getText()),
                          f -> (f.weight == null) ? 1F : Float.parseFloat(f.weight.getText()))));
      builder.add(new UnresolvedArgument("fields", fields));
    }

    // Query is always required
    builder.add(
        new UnresolvedArgument(
            "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));

    // Add optional arguments
    ctx.relevanceArg()
        .forEach(
            v ->
                builder.add(
                    new UnresolvedArgument(
                        v.relevanceArgName().getText().toLowerCase(Locale.ROOT),
                        new Literal(
                            StringUtils.unquoteText(v.relevanceArgValue().getText()),
                            DataType.STRING))));
    return builder.build();
  }

  // New visitor methods for spanValue grammar rules

  @Override
  public UnresolvedExpression visitNumericSpanValue(
      OpenSearchPPLParser.NumericSpanValueContext ctx) {
    String spanValue = ctx.literalValue().getText();
    String spanUnit = ctx.timespanUnit() != null ? ctx.timespanUnit().getText() : null;

    if (spanUnit != null) {
      // Create combined span like "1h", "30m", etc.
      return org.opensearch.sql.ast.dsl.AstDSL.stringLiteral(spanValue + spanUnit);
    } else {
      return visit(ctx.literalValue());
    }
  }

  @Override
  public UnresolvedExpression visitLogWithBaseSpan(OpenSearchPPLParser.LogWithBaseSpanContext ctx) {
    return org.opensearch.sql.ast.dsl.AstDSL.stringLiteral(ctx.getText());
  }

  // Visitor methods for search expressions
  @Override
  public SearchExpression visitGroupedExpression(OpenSearchPPLParser.GroupedExpressionContext ctx) {
    return new SearchGroup((SearchExpression) visit(ctx.searchExpression()));
  }

  @Override
  public SearchExpression visitNotExpression(OpenSearchPPLParser.NotExpressionContext ctx) {
    return new SearchNot((SearchExpression) visit(ctx.searchExpression()));
  }

  @Override
  public SearchExpression visitAndExpression(OpenSearchPPLParser.AndExpressionContext ctx) {
    SearchExpression left = (SearchExpression) visit(ctx.searchExpression(0));
    SearchExpression right = (SearchExpression) visit(ctx.searchExpression(1));
    // Wrap the entire AND expression in parentheses
    return new SearchGroup(new SearchAnd(left, right));
  }

  @Override
  public SearchExpression visitOrExpression(OpenSearchPPLParser.OrExpressionContext ctx) {
    SearchExpression left = (SearchExpression) visit(ctx.searchExpression(0));
    SearchExpression right = (SearchExpression) visit(ctx.searchExpression(1));
    // Wrap the entire OR expression in parentheses
    return new SearchGroup(new SearchOr(left, right));
  }

  @Override
  public SearchExpression visitTermExpression(OpenSearchPPLParser.TermExpressionContext ctx) {
    return (SearchExpression) visit(ctx.searchTerm());
  }

  @Override
  public SearchExpression visitSearchLiteralTerm(OpenSearchPPLParser.SearchLiteralTermContext ctx) {
    return visitSearchLiteral(ctx.searchLiteral());
  }

  @Override
  public SearchExpression visitSearchComparisonTerm(
      OpenSearchPPLParser.SearchComparisonTermContext ctx) {
    OpenSearchPPLParser.SearchFieldCompareContext fieldComp =
        (OpenSearchPPLParser.SearchFieldCompareContext) ctx.searchFieldComparison();

    Field field = (Field) visit(fieldComp.fieldExpression());
    SearchComparison.Operator op =
        visitSearchComparisonOperator(fieldComp.searchComparisonOperator());

    // Use SearchLiteral directly
    SearchLiteral searchLit = visitSearchLiteral(fieldComp.searchLiteral());

    return new SearchComparison(field, op, searchLit);
  }

  @Override
  public SearchExpression visitSearchInListTerm(OpenSearchPPLParser.SearchInListTermContext ctx) {
    OpenSearchPPLParser.SearchFieldInValuesContext fieldIn =
        (OpenSearchPPLParser.SearchFieldInValuesContext) ctx.searchFieldInList();

    Field field = (Field) visit(fieldIn.fieldExpression());
    OpenSearchPPLParser.SearchLiteralsContext valueList =
        (OpenSearchPPLParser.SearchLiteralsContext) fieldIn.searchLiteralList();
    List<SearchLiteral> values =
        valueList.searchLiteral().stream()
            .map(this::visitSearchLiteral)
            .collect(Collectors.toList());

    return new SearchIn(field, values);
  }

  // Helper method to determine the comparison operator
  private SearchComparison.Operator visitSearchComparisonOperator(
      OpenSearchPPLParser.SearchComparisonOperatorContext ctx) {
    if (ctx instanceof OpenSearchPPLParser.EqualsContext) {
      return SearchComparison.Operator.EQUALS;
    } else if (ctx instanceof OpenSearchPPLParser.NotEqualsContext) {
      return SearchComparison.Operator.NOT_EQUALS;
    } else if (ctx instanceof OpenSearchPPLParser.LessThanContext) {
      return SearchComparison.Operator.LESS_THAN;
    } else if (ctx instanceof OpenSearchPPLParser.LessOrEqualContext) {
      return SearchComparison.Operator.LESS_OR_EQUAL;
    } else if (ctx instanceof OpenSearchPPLParser.GreaterThanContext) {
      return SearchComparison.Operator.GREATER_THAN;
    } else if (ctx instanceof OpenSearchPPLParser.GreaterOrEqualContext) {
      return SearchComparison.Operator.GREATER_OR_EQUAL;
    }
    return SearchComparison.Operator.EQUALS; // Default to equals
  }

  @Override
  public SearchLiteral visitSearchLiteral(OpenSearchPPLParser.SearchLiteralContext ctx) {
    if (ctx.stringLiteral() != null) {
      // Use visit method to properly handle escaping
      Literal stringLit = (Literal) visit(ctx.stringLiteral());
      String content = (String) stringLit.getValue();
      return new SearchLiteral(new Literal(content, DataType.STRING), content.contains(" "));
    } else if (ctx.numericLiteral() != null) {
      Literal numericLiteral = (Literal) visit(ctx.numericLiteral());
      return new SearchLiteral(numericLiteral, false);
    } else if (ctx.booleanLiteral() != null) {
      // Boolean literal
      Literal booleanLiteral = (Literal) visit(ctx.booleanLiteral());
      return new SearchLiteral(booleanLiteral, false);
    } else if (ctx.ID() != null) {
      return new SearchLiteral(new Literal(ctx.ID().getText(), DataType.STRING), false);
    } else if (ctx.searchableKeyWord() != null) {
      return new SearchLiteral(
          new Literal(ctx.searchableKeyWord().getText(), DataType.STRING), false);
    }
    // Default
    return new SearchLiteral(new Literal(ctx.getText(), DataType.STRING), false);
  }
}
