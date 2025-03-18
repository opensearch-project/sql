/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POSITION;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BinaryArithmeticContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanFunctionCallContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BySpanClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.CompareExprContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.ConvertedDataTypeContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.CountAllFunctionCallContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DataTypeFunctionCallContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DecimalLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DistinctCountFunctionCallContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalFunctionCallContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldExpressionContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IdentsAsQualifiedNameContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IdentsAsTableQualifiedNameContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IdentsAsWildcardQualifiedNameContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.InExprContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntegerLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntervalLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalAndContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalNotContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalOrContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LogicalXorContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.MultiFieldRelevanceFunctionContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.ParentheticValueExprContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SingleFieldRelevanceFunctionContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortFieldContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SpanClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsFunctionCallContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StringLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TableSourceContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.WcFieldExpressionContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
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
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ppl.utils.ArgumentFactory;

/** Class of building AST Expression nodes. */
public class AstExpressionBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedExpression> {

  private static final int DEFAULT_TAKE_FUNCTION_SIZE_VALUE = 10;

  /** The function name mapping between fronted and core engine. */
  private static Map<String, String> FUNCTION_NAME_MAPPING =
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
    return new Let((Field) visit(ctx.fieldExpression()), visit(ctx.expression()));
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

  /** Comparison expression. */
  @Override
  public UnresolvedExpression visitCompareExpr(CompareExprContext ctx) {
    return new Compare(ctx.comparisonOperator().getText(), visit(ctx.left), visit(ctx.right));
  }

  @Override
  public UnresolvedExpression visitInExpr(InExprContext ctx) {
    UnresolvedExpression expr =
        new In(
            visit(ctx.valueExpression()),
            ctx.valueList().literalValue().stream()
                .map(this::visitLiteralValue)
                .collect(Collectors.toList()));
    return ctx.NOT() != null ? new Not(expr) : expr;
  }

  /** Value Expression. */
  @Override
  public UnresolvedExpression visitBinaryArithmetic(BinaryArithmeticContext ctx) {
    return new Function(
        ctx.binaryOperator.getText(), Arrays.asList(visit(ctx.left), visit(ctx.right)));
  }

  @Override
  public UnresolvedExpression visitParentheticValueExpr(ParentheticValueExprContext ctx) {
    return visit(ctx.valueExpression()); // Discard parenthesis around
  }

  @Override
  public UnresolvedExpression visitParentheticLogicalExpr(
      OpenSearchPPLParser.ParentheticLogicalExprContext ctx) {
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
  public UnresolvedExpression visitSortField(SortFieldContext ctx) {

    // TODO #3180: Fix broken sort functionality
    return new Field(
        visit(ctx.sortFieldExpression().fieldExpression().qualifiedName()),
        ArgumentFactory.getArgumentList(ctx));
  }

  /** Aggregation function. */
  @Override
  public UnresolvedExpression visitStatsFunctionCall(StatsFunctionCallContext ctx) {
    return new AggregateFunction(ctx.statsFunctionName().getText(), visit(ctx.valueExpression()));
  }

  @Override
  public UnresolvedExpression visitCountAllFunctionCall(CountAllFunctionCallContext ctx) {
    return new AggregateFunction("count", AllFields.of());
  }

  @Override
  public UnresolvedExpression visitDistinctCountFunctionCall(DistinctCountFunctionCallContext ctx) {
    return new AggregateFunction("count", visit(ctx.valueExpression()), true);
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

  /** Eval function. */
  @Override
  public UnresolvedExpression visitBooleanFunctionCall(BooleanFunctionCallContext ctx) {
    final String functionName = ctx.conditionFunctionName().getText().toLowerCase();
    return buildFunction(
        FUNCTION_NAME_MAPPING.getOrDefault(functionName, functionName),
        ctx.functionArgs().functionArg());
  }

  /** Eval function. */
  @Override
  public UnresolvedExpression visitEvalFunctionCall(EvalFunctionCallContext ctx) {
    return buildFunction(ctx.evalFunctionName().getText(), ctx.functionArgs().functionArg());
  }

  /** Cast function. */
  @Override
  public UnresolvedExpression visitDataTypeFunctionCall(DataTypeFunctionCallContext ctx) {
    return new Cast(visit(ctx.expression()), visit(ctx.convertedDataType()));
  }

  @Override
  public UnresolvedExpression visitConvertedDataType(ConvertedDataTypeContext ctx) {
    return AstDSL.stringLiteral(ctx.getText());
  }

  private Function buildFunction(
      String functionName, List<OpenSearchPPLParser.FunctionArgContext> args) {
    return new Function(
        functionName, args.stream().map(this::visitFunctionArg).collect(Collectors.toList()));
  }

  @Override
  public UnresolvedExpression visitSingleFieldRelevanceFunction(
      SingleFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.singleFieldRelevanceFunctionName().getText().toLowerCase(),
        singleFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitMultiFieldRelevanceFunction(
      MultiFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(),
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
  public UnresolvedExpression visitPositionFunction(
      OpenSearchPPLParser.PositionFunctionContext ctx) {
    return new Function(
        POSITION.getName().getFunctionName(),
        Arrays.asList(visitFunctionArg(ctx.functionArg(0)), visitFunctionArg(ctx.functionArg(1))));
  }

  @Override
  public UnresolvedExpression visitExtractFunctionCall(
      OpenSearchPPLParser.ExtractFunctionCallContext ctx) {
    return new Function(
        ctx.extractFunction().EXTRACT().toString(), getExtractFunctionArguments(ctx));
  }

  private List<UnresolvedExpression> getExtractFunctionArguments(
      OpenSearchPPLParser.ExtractFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.extractFunction().datetimePart().getText(), DataType.STRING),
            visitFunctionArg(ctx.extractFunction().functionArg()));
    return args;
  }

  @Override
  public UnresolvedExpression visitGetFormatFunctionCall(
      OpenSearchPPLParser.GetFormatFunctionCallContext ctx) {
    return new Function(
        ctx.getFormatFunction().GET_FORMAT().toString(), getFormatFunctionArguments(ctx));
  }

  private List<UnresolvedExpression> getFormatFunctionArguments(
      OpenSearchPPLParser.GetFormatFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.getFormatFunction().getFormatType().getText(), DataType.STRING),
            visitFunctionArg(ctx.getFormatFunction().functionArg()));
    return args;
  }

  @Override
  public UnresolvedExpression visitTimestampFunctionCall(
      OpenSearchPPLParser.TimestampFunctionCallContext ctx) {
    return new Function(
        ctx.timestampFunction().timestampFunctionName().getText(), timestampFunctionArguments(ctx));
  }

  private List<UnresolvedExpression> timestampFunctionArguments(
      OpenSearchPPLParser.TimestampFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.timestampFunction().simpleDateTimePart().getText(), DataType.STRING),
            visitFunctionArg(ctx.timestampFunction().firstArg),
            visitFunctionArg(ctx.timestampFunction().secondArg));
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
    return new Literal(Double.valueOf(ctx.getText()), DataType.DOUBLE);
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
    UnresolvedExpression expr =
        new InSubquery(
            ctx.valueExpressionList().valueExpression().stream()
                .map(this::visit)
                .collect(Collectors.toList()),
            astBuilder.visitSubSearch(ctx.subSearch()));
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
        new Between(
            visit(ctx.valueExpression(0)),
            visit(ctx.valueExpression(1)),
            visit(ctx.valueExpression(2)));
    return ctx.NOT() != null ? new Not(betweenExpr) : betweenExpr;
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
                        v.relevanceArgName().getText().toLowerCase(),
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
    var fields =
        new RelevanceFieldList(
            ctx.getRuleContexts(OpenSearchPPLParser.RelevanceFieldAndWeightContext.class).stream()
                .collect(
                    Collectors.toMap(
                        f -> StringUtils.unquoteText(f.field.getText()),
                        f -> (f.weight == null) ? 1F : Float.parseFloat(f.weight.getText()))));
    builder.add(new UnresolvedArgument("fields", fields));
    builder.add(
        new UnresolvedArgument(
            "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    ctx.relevanceArg()
        .forEach(
            v ->
                builder.add(
                    new UnresolvedArgument(
                        v.relevanceArgName().getText().toLowerCase(),
                        new Literal(
                            StringUtils.unquoteText(v.relevanceArgValue().getText()),
                            DataType.STRING))));
    return builder.build();
  }
}
