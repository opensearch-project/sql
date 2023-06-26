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
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.PercentileAggFunctionContext;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.IntervalUnit;
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
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ppl.utils.ArgumentFactory;

/**
 * Class of building AST Expression nodes.
 */
public class AstExpressionBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedExpression> {

  private static final int DEFAULT_TAKE_FUNCTION_SIZE_VALUE = 10;

  /**
   * The function name mapping between fronted and core engine.
   */
  private static Map<String, String> FUNCTION_NAME_MAPPING =
      new ImmutableMap.Builder<String, String>()
          .put("isnull", IS_NULL.getName().getFunctionName())
          .put("isnotnull", IS_NOT_NULL.getName().getFunctionName())
          .build();

  /**
   * Eval clause.
   */
  @Override
  public UnresolvedExpression visitEvalClause(EvalClauseContext ctx) {
    return new Let((Field) visit(ctx.fieldExpression()), visit(ctx.expression()));
  }

  /**
   * Logical expression excluding boolean, comparison.
   */
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

  /**
   * Comparison expression.
   */
  @Override
  public UnresolvedExpression visitCompareExpr(CompareExprContext ctx) {
    return new Compare(ctx.comparisonOperator().getText(), visit(ctx.left), visit(ctx.right));
  }

  @Override
  public UnresolvedExpression visitInExpr(InExprContext ctx) {
    return new In(
        visit(ctx.valueExpression()),
        ctx.valueList()
            .literalValue()
            .stream()
            .map(this::visitLiteralValue)
            .collect(Collectors.toList()));
  }

  /**
   * Value Expression.
   */
  @Override
  public UnresolvedExpression visitBinaryArithmetic(BinaryArithmeticContext ctx) {
    return new Function(
        ctx.binaryOperator.getText(),
        Arrays.asList(visit(ctx.left), visit(ctx.right))
    );
  }

  @Override
  public UnresolvedExpression visitParentheticValueExpr(ParentheticValueExprContext ctx) {
    return visit(ctx.valueExpression()); // Discard parenthesis around
  }

  /**
   * Field expression.
   */
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
    return new Field(
        visit(ctx.sortFieldExpression().fieldExpression().qualifiedName()),
        ArgumentFactory.getArgumentList(ctx)
    );
  }

  /**
   * Aggregation function.
   */
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
  public UnresolvedExpression visitPercentileAggFunction(PercentileAggFunctionContext ctx) {
    return new AggregateFunction(ctx.PERCENTILE().getText(), visit(ctx.aggField),
        Collections.singletonList(new Argument("rank", (Literal) visit(ctx.value))));
  }

  @Override
  public UnresolvedExpression visitTakeAggFunctionCall(
      OpenSearchPPLParser.TakeAggFunctionCallContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("size",
        ctx.takeAggFunction().size != null ? visit(ctx.takeAggFunction().size) :
            AstDSL.intLiteral(DEFAULT_TAKE_FUNCTION_SIZE_VALUE)));
    return new AggregateFunction("take", visit(ctx.takeAggFunction().fieldExpression()),
        builder.build());
  }

  /**
   * Eval function.
   */
  @Override
  public UnresolvedExpression visitBooleanFunctionCall(BooleanFunctionCallContext ctx) {
    final String functionName = ctx.conditionFunctionBase().getText();
    return buildFunction(FUNCTION_NAME_MAPPING.getOrDefault(functionName, functionName),
        ctx.functionArgs().functionArg());
  }

  /**
   * Eval function.
   */
  @Override
  public UnresolvedExpression visitEvalFunctionCall(EvalFunctionCallContext ctx) {
    return buildFunction(ctx.evalFunctionName().getText(), ctx.functionArgs().functionArg());
  }

  /**
   * Cast function.
   */
  @Override
  public UnresolvedExpression visitDataTypeFunctionCall(DataTypeFunctionCallContext ctx) {
    return new Cast(visit(ctx.expression()), visit(ctx.convertedDataType()));
  }

  @Override
  public UnresolvedExpression visitConvertedDataType(ConvertedDataTypeContext ctx) {
    return AstDSL.stringLiteral(ctx.getText());
  }

  private Function buildFunction(String functionName,
                                 List<OpenSearchPPLParser.FunctionArgContext> args) {
    return new Function(
        functionName,
        args
            .stream()
            .map(this::visitFunctionArg)
            .collect(Collectors.toList())
    );
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
            Arrays.asList(visitFunctionArg(ctx.functionArg(0)),
                    visitFunctionArg(ctx.functionArg(1))));
  }

  /**
   * Literal and value.
   */
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
    return ctx.alias != null ? new Alias(name, visit(ctx.spanClause()), StringUtils
        .unquoteIdentifier(ctx.alias.getText())) : new Alias(name, visit(ctx.spanClause()));
  }

  @Override
  public UnresolvedExpression visitSpanClause(SpanClauseContext ctx) {
    String unit = ctx.unit != null ? ctx.unit.getText() : "";
    return new Span(visit(ctx.fieldExpression()), visit(ctx.value), SpanUnit.of(unit));
  }

  private QualifiedName visitIdentifiers(List<? extends ParserRuleContext> ctx) {
    return new QualifiedName(
        ctx.stream()
            .map(RuleContext::getText)
            .map(StringUtils::unquoteIdentifier)
            .collect(Collectors.toList())
    );
  }

  private List<UnresolvedExpression> singleFieldRelevanceArguments(
      SingleFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("field",
        new QualifiedName(StringUtils.unquoteText(ctx.field.getText()))));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    ctx.relevanceArg().forEach(v -> builder.add(new UnresolvedArgument(
        v.relevanceArgName().getText().toLowerCase(), new Literal(StringUtils.unquoteText(
        v.relevanceArgValue().getText()), DataType.STRING))));
    return builder.build();
  }

  private List<UnresolvedExpression> multiFieldRelevanceArguments(
      MultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    var fields = new RelevanceFieldList(ctx
        .getRuleContexts(OpenSearchPPLParser.RelevanceFieldAndWeightContext.class)
        .stream()
        .collect(Collectors.toMap(
            f -> StringUtils.unquoteText(f.field.getText()),
            f -> (f.weight == null) ? 1F : Float.parseFloat(f.weight.getText()))));
    builder.add(new UnresolvedArgument("fields", fields));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    ctx.relevanceArg().forEach(v -> builder.add(new UnresolvedArgument(
        v.relevanceArgName().getText().toLowerCase(), new Literal(StringUtils.unquoteText(
        v.relevanceArgValue().getText()), DataType.STRING))));
    return builder.build();
  }

}
