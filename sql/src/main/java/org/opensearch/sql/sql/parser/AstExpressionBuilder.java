/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.between;
import static org.opensearch.sql.ast.dsl.AstDSL.not;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LIKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOT_LIKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POSITION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REGEXP;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AltMultiFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AltSingleFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AlternateMultiMatchFieldContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.BetweenPredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.BinaryComparisonPredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.BooleanContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.CaseFuncAlternativeContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.CaseFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ColumnFilterContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ConvertedDataTypeContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.CountStarFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.DataTypeFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.DateLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.DistinctCountFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ExtractFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FilterClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FilteredAggregationFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FunctionArgContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.GetFormatFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.HighlightFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.InPredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.IsNullPredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.LikePredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.MathExpressionAtomContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.MultiFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NestedAllFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NoFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NotExpressionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NullLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OverClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.PositionFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QualifiedNameContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RegexpPredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RegularAggregateFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RelevanceArgContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RelevanceFieldAndWeightContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScalarFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScalarWindowFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScoreRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ShowDescribePatternContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SignedDecimalContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SignedRealContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SingleFieldRelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.StringContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.StringLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableFilterContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TimeLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TimestampFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TimestampLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.WindowFunctionClauseContext;
import static org.opensearch.sql.sql.parser.ParserUtils.createSortOption;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.RuleContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.*;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AlternateMultiMatchQueryContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AndExpressionContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ColumnNameContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.IdentContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.IntervalLiteralContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NestedExpressionAtomContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OrExpressionContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableNameContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;

/** Expression builder to parse text to expression in AST. */
public class AstExpressionBuilder extends OpenSearchSQLParserBaseVisitor<UnresolvedExpression> {

  @Override
  public UnresolvedExpression visitTableName(TableNameContext ctx) {
    return visit(ctx.qualifiedName());
  }

  @Override
  public UnresolvedExpression visitColumnName(ColumnNameContext ctx) {
    return visit(ctx.qualifiedName());
  }

  @Override
  public UnresolvedExpression visitIdent(IdentContext ctx) {
    return visitIdentifiers(Collections.singletonList(ctx));
  }

  @Override
  public UnresolvedExpression visitQualifiedName(QualifiedNameContext ctx) {
    return visitIdentifiers(ctx.ident());
  }

  @Override
  public UnresolvedExpression visitMathExpressionAtom(MathExpressionAtomContext ctx) {
    return new Function(
        ctx.mathOperator.getText(), Arrays.asList(visit(ctx.left), visit(ctx.right)));
  }

  @Override
  public UnresolvedExpression visitNestedExpressionAtom(NestedExpressionAtomContext ctx) {
    return visit(ctx.expression()); // Discard parenthesis around
  }

  @Override
  public UnresolvedExpression visitNestedAllFunctionCall(NestedAllFunctionCallContext ctx) {
    return new NestedAllTupleFields(visitQualifiedName(ctx.allTupleFields().path).toString());
  }

  @Override
  public UnresolvedExpression visitScalarFunctionCall(ScalarFunctionCallContext ctx) {
    return buildFunction(ctx.scalarFunctionName().getText(), ctx.functionArgs().functionArg());
  }

  @Override
  public UnresolvedExpression visitGetFormatFunctionCall(GetFormatFunctionCallContext ctx) {
    return new Function(
        ctx.getFormatFunction().GET_FORMAT().toString(), getFormatFunctionArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitHighlightFunctionCall(HighlightFunctionCallContext ctx) {
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.highlightFunction()
        .highlightArg()
        .forEach(
            v ->
                builder.put(
                    v.highlightArgName().getText().toLowerCase(Locale.ROOT),
                    new Literal(
                        StringUtils.unquoteText(v.highlightArgValue().getText()),
                        DataType.STRING)));

    return new HighlightFunction(visit(ctx.highlightFunction().relevanceField()), builder.build());
  }

  @Override
  public UnresolvedExpression visitTimestampFunctionCall(TimestampFunctionCallContext ctx) {
    return new Function(
        ctx.timestampFunction().timestampFunctionName().getText(), timestampFunctionArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitPositionFunction(PositionFunctionContext ctx) {
    return new Function(
        POSITION.getName().getFunctionName(),
        Arrays.asList(visitFunctionArg(ctx.functionArg(0)), visitFunctionArg(ctx.functionArg(1))));
  }

  @Override
  public UnresolvedExpression visitTableFilter(TableFilterContext ctx) {
    return new Function(
        LIKE.getName().getFunctionName(),
        Arrays.asList(qualifiedName("TABLE_NAME"), visit(ctx.showDescribePattern())));
  }

  @Override
  public UnresolvedExpression visitColumnFilter(ColumnFilterContext ctx) {
    return new Function(
        LIKE.getName().getFunctionName(),
        Arrays.asList(qualifiedName("COLUMN_NAME"), visit(ctx.showDescribePattern())));
  }

  @Override
  public UnresolvedExpression visitShowDescribePattern(ShowDescribePatternContext ctx) {
    return visit(ctx.stringLiteral());
  }

  @Override
  public UnresolvedExpression visitFilteredAggregationFunctionCall(
      FilteredAggregationFunctionCallContext ctx) {
    AggregateFunction agg = (AggregateFunction) visit(ctx.aggregateFunction());
    return agg.condition(visit(ctx.filterClause()));
  }

  @Override
  public UnresolvedExpression visitWindowFunctionClause(WindowFunctionClauseContext ctx) {
    OverClauseContext overClause = ctx.overClause();

    List<UnresolvedExpression> partitionByList = Collections.emptyList();
    if (overClause.partitionByClause() != null) {
      partitionByList =
          overClause.partitionByClause().expression().stream()
              .map(this::visit)
              .collect(Collectors.toList());
    }

    List<Pair<SortOption, UnresolvedExpression>> sortList = Collections.emptyList();
    if (overClause.orderByClause() != null) {
      sortList =
          overClause.orderByClause().orderByElement().stream()
              .map(item -> ImmutablePair.of(createSortOption(item), visit(item.expression())))
              .collect(Collectors.toList());
    }
    return new WindowFunction(visit(ctx.function), partitionByList, sortList);
  }

  @Override
  public UnresolvedExpression visitScalarWindowFunction(ScalarWindowFunctionContext ctx) {
    return buildFunction(ctx.functionName.getText(), ctx.functionArgs().functionArg());
  }

  @Override
  public UnresolvedExpression visitRegularAggregateFunctionCall(
      RegularAggregateFunctionCallContext ctx) {
    return new AggregateFunction(ctx.functionName.getText(), visitFunctionArg(ctx.functionArg()));
  }

  @Override
  public UnresolvedExpression visitDistinctCountFunctionCall(DistinctCountFunctionCallContext ctx) {
    return new AggregateFunction(ctx.COUNT().getText(), visitFunctionArg(ctx.functionArg()), true);
  }

  @Override
  public UnresolvedExpression visitCountStarFunctionCall(CountStarFunctionCallContext ctx) {
    return new AggregateFunction("COUNT", AllFields.of());
  }

  @Override
  public UnresolvedExpression visitFilterClause(FilterClauseContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public UnresolvedExpression visitIsNullPredicate(IsNullPredicateContext ctx) {
    return new Function(
        ctx.nullNotnull().NOT() == null
            ? IS_NULL.getName().getFunctionName()
            : IS_NOT_NULL.getName().getFunctionName(),
        Arrays.asList(visit(ctx.predicate())));
  }

  @Override
  public UnresolvedExpression visitBetweenPredicate(BetweenPredicateContext ctx) {
    UnresolvedExpression func =
        between(visit(ctx.predicate(0)), visit(ctx.predicate(1)), visit(ctx.predicate(2)));

    if (ctx.NOT() != null) {
      func = not(func);
    }
    return func;
  }

  @Override
  public UnresolvedExpression visitLikePredicate(LikePredicateContext ctx) {
    return new Function(
        ctx.NOT() == null ? LIKE.getName().getFunctionName() : NOT_LIKE.getName().getFunctionName(),
        Arrays.asList(visit(ctx.left), visit(ctx.right)));
  }

  @Override
  public UnresolvedExpression visitRegexpPredicate(RegexpPredicateContext ctx) {
    return new Function(
        REGEXP.getName().getFunctionName(), Arrays.asList(visit(ctx.left), visit(ctx.right)));
  }

  @Override
  public UnresolvedExpression visitInPredicate(InPredicateContext ctx) {
    UnresolvedExpression field = visit(ctx.predicate());
    List<UnresolvedExpression> inLists =
        ctx.expressions().expression().stream().map(this::visit).collect(Collectors.toList());
    UnresolvedExpression in = AstDSL.in(field, inLists);
    return ctx.NOT() != null ? AstDSL.not(in) : in;
  }

  @Override
  public UnresolvedExpression visitAndExpression(AndExpressionContext ctx) {
    return new And(visit(ctx.left), visit(ctx.right));
  }

  @Override
  public UnresolvedExpression visitOrExpression(OrExpressionContext ctx) {
    return new Or(visit(ctx.left), visit(ctx.right));
  }

  @Override
  public UnresolvedExpression visitNotExpression(NotExpressionContext ctx) {
    return new Not(visit(ctx.expression()));
  }

  @Override
  public UnresolvedExpression visitString(StringContext ctx) {
    return AstDSL.stringLiteral(StringUtils.unquoteText(ctx.getText()));
  }

  @Override
  public UnresolvedExpression visitSignedDecimal(SignedDecimalContext ctx) {
    long number = Long.parseLong(ctx.getText());
    if (Integer.MIN_VALUE <= number && number <= Integer.MAX_VALUE) {
      return AstDSL.intLiteral((int) number);
    }
    return AstDSL.longLiteral(number);
  }

  @Override
  public UnresolvedExpression visitSignedReal(SignedRealContext ctx) {
    return AstDSL.doubleLiteral(Double.valueOf(ctx.getText()));
  }

  @Override
  public UnresolvedExpression visitBoolean(BooleanContext ctx) {
    return AstDSL.booleanLiteral(Boolean.valueOf(ctx.getText()));
  }

  @Override
  public UnresolvedExpression visitStringLiteral(StringLiteralContext ctx) {
    return AstDSL.stringLiteral(StringUtils.unquoteText(ctx.getText()));
  }

  @Override
  public UnresolvedExpression visitNullLiteral(NullLiteralContext ctx) {
    return AstDSL.nullLiteral();
  }

  @Override
  public UnresolvedExpression visitDateLiteral(DateLiteralContext ctx) {
    return AstDSL.dateLiteral(StringUtils.unquoteText(ctx.date.getText()));
  }

  @Override
  public UnresolvedExpression visitTimeLiteral(TimeLiteralContext ctx) {
    return AstDSL.timeLiteral(StringUtils.unquoteText(ctx.time.getText()));
  }

  @Override
  public UnresolvedExpression visitTimestampLiteral(TimestampLiteralContext ctx) {
    return AstDSL.timestampLiteral(StringUtils.unquoteText(ctx.timestamp.getText()));
  }

  @Override
  public UnresolvedExpression visitIntervalLiteral(IntervalLiteralContext ctx) {
    return new Interval(visit(ctx.expression()), IntervalUnit.of(ctx.intervalUnit().getText()));
  }

  @Override
  public UnresolvedExpression visitBinaryComparisonPredicate(BinaryComparisonPredicateContext ctx) {
    String functionName = ctx.comparisonOperator().getText();
    return new Function(
        functionName.equals("<>") ? "!=" : functionName,
        Arrays.asList(visit(ctx.left), visit(ctx.right)));
  }

  @Override
  public UnresolvedExpression visitCaseFunctionCall(CaseFunctionCallContext ctx) {
    UnresolvedExpression caseValue = (ctx.expression() == null) ? null : visit(ctx.expression());
    List<When> whenStatements =
        ctx.caseFuncAlternative().stream()
            .map(when -> (When) visit(when))
            .collect(Collectors.toList());
    UnresolvedExpression elseStatement = (ctx.elseArg == null) ? null : visit(ctx.elseArg);

    return new Case(caseValue, whenStatements, Optional.ofNullable(elseStatement));
  }

  @Override
  public UnresolvedExpression visitCaseFuncAlternative(CaseFuncAlternativeContext ctx) {
    return new When(visit(ctx.condition), visit(ctx.consequent));
  }

  @Override
  public UnresolvedExpression visitDataTypeFunctionCall(DataTypeFunctionCallContext ctx) {
    return new Cast(visit(ctx.expression()), visit(ctx.convertedDataType()));
  }

  @Override
  public UnresolvedExpression visitConvertedDataType(ConvertedDataTypeContext ctx) {
    return AstDSL.stringLiteral(ctx.getText());
  }

  @Override
  public UnresolvedExpression visitPercentileApproxFunctionCall(
      OpenSearchSQLParser.PercentileApproxFunctionCallContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(
        new UnresolvedArgument(
            "percent",
            AstDSL.doubleLiteral(
                Double.valueOf(ctx.percentileApproxFunction().percent.getText()))));
    if (ctx.percentileApproxFunction().compression != null) {
      builder.add(
          new UnresolvedArgument(
              "compression",
              AstDSL.doubleLiteral(
                  Double.valueOf(ctx.percentileApproxFunction().compression.getText()))));
    }
    return new AggregateFunction(
        "percentile", visit(ctx.percentileApproxFunction().aggField), builder.build());
  }

  @Override
  public UnresolvedExpression visitNoFieldRelevanceFunction(NoFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.noFieldRelevanceFunctionName().getText().toLowerCase(Locale.ROOT),
        noFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitSingleFieldRelevanceFunction(
      SingleFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.singleFieldRelevanceFunctionName().getText().toLowerCase(Locale.ROOT),
        singleFieldRelevanceArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitAltSingleFieldRelevanceFunction(
      AltSingleFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.altSyntaxFunctionName.getText().toLowerCase(Locale.ROOT),
        altSingleFieldRelevanceFunctionArguments(ctx));
  }

  @Override
  public UnresolvedExpression visitMultiFieldRelevanceFunction(
      MultiFieldRelevanceFunctionContext ctx) {
    // To support alternate syntax for MULTI_MATCH like
    // 'MULTI_MATCH('query'='query_val', 'fields'='*fields_val')'
    String funcName = StringUtils.unquoteText(ctx.multiFieldRelevanceFunctionName().getText());
    if ((funcName.equalsIgnoreCase(BuiltinFunctionName.MULTI_MATCH.toString())
            || funcName.equalsIgnoreCase(BuiltinFunctionName.MULTIMATCH.toString())
            || funcName.equalsIgnoreCase(BuiltinFunctionName.MULTIMATCHQUERY.toString()))
        && !ctx.getRuleContexts(AlternateMultiMatchQueryContext.class).isEmpty()) {
      return new Function(
          ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(Locale.ROOT),
          alternateMultiMatchArguments(ctx));
    } else {
      return new Function(
          ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(Locale.ROOT),
          multiFieldRelevanceArguments(ctx));
    }
  }

  @Override
  public UnresolvedExpression visitAltMultiFieldRelevanceFunction(
      AltMultiFieldRelevanceFunctionContext ctx) {
    return new Function(
        ctx.altSyntaxFunctionName.getText().toLowerCase(Locale.ROOT),
        altMultiFieldRelevanceFunctionArguments(ctx));
  }

  /**
   * Visit score-relevance function and collect children.
   *
   * @param ctx the parse tree
   * @return children
   */
  public UnresolvedExpression visitScoreRelevanceFunction(ScoreRelevanceFunctionContext ctx) {
    Literal weight =
        ctx.weight == null
            ? new Literal(Double.valueOf(1.0), DataType.DOUBLE)
            : new Literal(Double.parseDouble(ctx.weight.getText()), DataType.DOUBLE);
    return new ScoreFunction(visit(ctx.relevanceFunction()), weight);
  }

  private Function buildFunction(String functionName, List<FunctionArgContext> arg) {
    return new Function(
        functionName, arg.stream().map(this::visitFunctionArg).collect(Collectors.toList()));
  }

  @Override
  public UnresolvedExpression visitExtractFunctionCall(ExtractFunctionCallContext ctx) {
    return new Function(
        ctx.extractFunction().EXTRACT().toString(), getExtractFunctionArguments(ctx));
  }

  private QualifiedName visitIdentifiers(List<IdentContext> identifiers) {
    return new QualifiedName(
        identifiers.stream()
            .map(RuleContext::getText)
            .map(StringUtils::unquoteIdentifier)
            .collect(Collectors.toList()));
  }

  private void fillRelevanceArgs(
      List<RelevanceArgContext> args, ImmutableList.Builder<UnresolvedExpression> builder) {
    // To support old syntax we must support argument keys as quoted strings.
    args.forEach(
        v ->
            builder.add(
                v.argName == null
                    ? new UnresolvedArgument(
                        v.relevanceArgName().getText().toLowerCase(Locale.ROOT),
                        new Literal(
                            StringUtils.unquoteText(v.relevanceArgValue().getText()),
                            DataType.STRING))
                    : new UnresolvedArgument(
                        StringUtils.unquoteText(v.argName.getText()).toLowerCase(Locale.ROOT),
                        new Literal(
                            StringUtils.unquoteText(v.argVal.getText()), DataType.STRING))));
  }

  private List<UnresolvedExpression> noFieldRelevanceArguments(
      NoFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(
        new UnresolvedArgument(
            "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
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
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }

  private List<UnresolvedExpression> altSingleFieldRelevanceFunctionArguments(
      AltSingleFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(
        new UnresolvedArgument(
            "field", new QualifiedName(StringUtils.unquoteText(ctx.field.getText()))));
    builder.add(
        new UnresolvedArgument(
            "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }

  private List<UnresolvedExpression> multiFieldRelevanceArguments(
      MultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    var fields =
        new RelevanceFieldList(
            ctx.getRuleContexts(RelevanceFieldAndWeightContext.class).stream()
                .collect(
                    Collectors.toMap(
                        f -> StringUtils.unquoteText(f.field.getText()),
                        f -> (f.weight == null) ? 1F : Float.parseFloat(f.weight.getText()))));
    builder.add(new UnresolvedArgument("fields", fields));
    builder.add(
        new UnresolvedArgument(
            "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }

  private List<UnresolvedExpression> getFormatFunctionArguments(GetFormatFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.getFormatFunction().getFormatType().getText(), DataType.STRING),
            visitFunctionArg(ctx.getFormatFunction().functionArg()));
    return args;
  }

  private List<UnresolvedExpression> timestampFunctionArguments(TimestampFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.timestampFunction().simpleDateTimePart().getText(), DataType.STRING),
            visitFunctionArg(ctx.timestampFunction().firstArg),
            visitFunctionArg(ctx.timestampFunction().secondArg));
    return args;
  }

  /**
   *
   *
   * <pre>
   * Adds support for multi_match alternate syntax like
   * MULTI_MATCH('query'='Dale', 'fields'='*name').
   * </pre>
   *
   * @param ctx : Context for multi field relevance function.
   * @return : Returns list of all arguments for relevance function.
   */
  private List<UnresolvedExpression> alternateMultiMatchArguments(
      MultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    Map<String, Float> fieldAndWeightMap = new HashMap<>();

    String[] fieldAndWeights =
        StringUtils.unquoteText(
                ctx.getRuleContexts(AlternateMultiMatchFieldContext.class).stream()
                    .findFirst()
                    .get()
                    .argVal
                    .getText())
            .split(",");

    for (var fieldAndWeight : fieldAndWeights) {
      String[] splitFieldAndWeights = fieldAndWeight.split("\\^");
      fieldAndWeightMap.put(
          splitFieldAndWeights[0],
          splitFieldAndWeights.length > 1 ? Float.parseFloat(splitFieldAndWeights[1]) : 1F);
    }
    builder.add(new UnresolvedArgument("fields", new RelevanceFieldList(fieldAndWeightMap)));

    ctx.getRuleContexts(AlternateMultiMatchQueryContext.class).stream()
        .findFirst()
        .ifPresent(
            arg ->
                builder.add(
                    new UnresolvedArgument(
                        "query",
                        new Literal(
                            StringUtils.unquoteText(arg.argVal.getText()), DataType.STRING))));

    fillRelevanceArgs(ctx.relevanceArg(), builder);

    return builder.build();
  }

  private List<UnresolvedExpression> altMultiFieldRelevanceFunctionArguments(
      AltMultiFieldRelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    var map = new HashMap<String, Float>();
    map.put(ctx.field.getText(), 1F);
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    var fields = new RelevanceFieldList(map);
    builder.add(new UnresolvedArgument("fields", fields));
    builder.add(
        new UnresolvedArgument(
            "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    fillRelevanceArgs(ctx.relevanceArg(), builder);
    return builder.build();
  }

  private List<UnresolvedExpression> getExtractFunctionArguments(ExtractFunctionCallContext ctx) {
    List<UnresolvedExpression> args =
        Arrays.asList(
            new Literal(ctx.extractFunction().datetimePart().getText(), DataType.STRING),
            visitFunctionArg(ctx.extractFunction().functionArg()));
    return args;
  }
}
