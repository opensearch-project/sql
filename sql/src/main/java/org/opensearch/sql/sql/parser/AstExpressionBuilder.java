/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LIKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.NOT_LIKE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.REGEXP;
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
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.IsNullPredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.LikePredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.MathExpressionAtomContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NotExpressionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NullLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OverClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QualifiedNameContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RegexpPredicateContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RegularAggregateFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.RelevanceFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScalarFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScalarWindowFunctionContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ShowDescribePatternContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SignedDecimalContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SignedRealContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.StringContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.StringLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableFilterContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TimeLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TimestampLiteralContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.WindowFunctionClauseContext;
import static org.opensearch.sql.sql.parser.ParserUtils.createSortOption;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.RuleContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AndExpressionContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ColumnNameContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FunctionArgsContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.IdentContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.IntervalLiteralContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.NestedExpressionAtomContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OrExpressionContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableNameContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;

/**
 * Expression builder to parse text to expression in AST.
 */
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
        ctx.mathOperator().getText(),
        Arrays.asList(visit(ctx.left), visit(ctx.right))
    );
  }

  @Override
  public UnresolvedExpression visitNestedExpressionAtom(NestedExpressionAtomContext ctx) {
    return visit(ctx.expression()); // Discard parenthesis around
  }

  @Override
  public UnresolvedExpression visitScalarFunctionCall(ScalarFunctionCallContext ctx) {
    return visitFunction(ctx.scalarFunctionName().getText(), ctx.functionArgs());
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
  public UnresolvedExpression visitShowDescribePattern(
      ShowDescribePatternContext ctx) {
    if (ctx.compatibleID() != null) {
      return stringLiteral(ctx.compatibleID().getText());
    } else {
      return visit(ctx.stringLiteral());
    }
  }

  @Override
  public UnresolvedExpression visitFilteredAggregationFunctionCall(
      OpenSearchSQLParser.FilteredAggregationFunctionCallContext ctx) {
    AggregateFunction agg = (AggregateFunction) visit(ctx.aggregateFunction());
    return agg.condition(visit(ctx.filterClause()));
  }

  @Override
  public UnresolvedExpression visitWindowFunctionClause(WindowFunctionClauseContext ctx) {
    OverClauseContext overClause = ctx.overClause();

    List<UnresolvedExpression> partitionByList = Collections.emptyList();
    if (overClause.partitionByClause() != null) {
      partitionByList = overClause.partitionByClause()
                                  .expression()
                                  .stream()
                                  .map(this::visit)
                                  .collect(Collectors.toList());
    }

    List<Pair<SortOption, UnresolvedExpression>> sortList = Collections.emptyList();
    if (overClause.orderByClause() != null) {
      sortList = overClause.orderByClause()
                           .orderByElement()
                           .stream()
                           .map(item -> ImmutablePair.of(
                               createSortOption(item), visit(item.expression())))
                           .collect(Collectors.toList());
    }
    return new WindowFunction(visit(ctx.function), partitionByList, sortList);
  }

  @Override
  public UnresolvedExpression visitScalarWindowFunction(ScalarWindowFunctionContext ctx) {
    return visitFunction(ctx.functionName.getText(), ctx.functionArgs());
  }

  @Override
  public UnresolvedExpression visitRegularAggregateFunctionCall(
      RegularAggregateFunctionCallContext ctx) {
    return new AggregateFunction(
        ctx.functionName.getText(),
        visitFunctionArg(ctx.functionArg()));
  }

  @Override
  public UnresolvedExpression visitDistinctCountFunctionCall(DistinctCountFunctionCallContext ctx) {
    return new AggregateFunction(
        ctx.COUNT().getText(),
        visitFunctionArg(ctx.functionArg()),
        true);
  }

  @Override
  public UnresolvedExpression visitCountStarFunctionCall(CountStarFunctionCallContext ctx) {
    return new AggregateFunction("COUNT", AllFields.of());
  }

  @Override
  public UnresolvedExpression visitFilterClause(OpenSearchSQLParser.FilterClauseContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public UnresolvedExpression visitIsNullPredicate(IsNullPredicateContext ctx) {
    return new Function(
        ctx.nullNotnull().NOT() == null ? IS_NULL.getName().getFunctionName() :
            IS_NOT_NULL.getName().getFunctionName(),
        Arrays.asList(visit(ctx.predicate())));
  }

  @Override
  public UnresolvedExpression visitLikePredicate(LikePredicateContext ctx) {
    return new Function(
        ctx.NOT() == null ? LIKE.getName().getFunctionName() :
            NOT_LIKE.getName().getFunctionName(),
        Arrays.asList(visit(ctx.left), visit(ctx.right)));
  }

  @Override
  public UnresolvedExpression visitRegexpPredicate(RegexpPredicateContext ctx) {
    return new Function(REGEXP.getName().getFunctionName(),
            Arrays.asList(visit(ctx.left), visit(ctx.right)));
  }

  @Override
  public UnresolvedExpression visitInList(OpenSearchSQLParser.InListContext ctx) {
    UnresolvedExpression field = visit(ctx.predicate());
    List<UnresolvedExpression> inLists =
        ctx.predicateList().predicate().stream().map(this::visit).collect(Collectors.toList());
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
  public UnresolvedExpression visitTimestampLiteral(
      TimestampLiteralContext ctx) {
    return AstDSL.timestampLiteral(StringUtils.unquoteText(ctx.timestamp.getText()));
  }

  @Override
  public UnresolvedExpression visitIntervalLiteral(IntervalLiteralContext ctx) {
    return new Interval(
        visit(ctx.expression()), IntervalUnit.of(ctx.intervalUnit().getText()));
  }

  @Override
  public UnresolvedExpression visitBinaryComparisonPredicate(
      BinaryComparisonPredicateContext ctx) {
    String functionName = ctx.comparisonOperator().getText();
    return new Function(
        functionName.equals("<>") ? "!=" : functionName,
        Arrays.asList(visit(ctx.left), visit(ctx.right))
    );
  }

  @Override
  public UnresolvedExpression visitCaseFunctionCall(CaseFunctionCallContext ctx) {
    UnresolvedExpression caseValue = (ctx.expression() == null) ? null : visit(ctx.expression());
    List<When> whenStatements = ctx.caseFuncAlternative()
                                   .stream()
                                   .map(when -> (When) visit(when))
                                   .collect(Collectors.toList());
    UnresolvedExpression elseStatement = (ctx.elseArg == null) ? null : visit(ctx.elseArg);

    return new Case(caseValue, whenStatements, elseStatement);
  }

  @Override
  public UnresolvedExpression visitCaseFuncAlternative(CaseFuncAlternativeContext ctx) {
    return new When(visit(ctx.condition), visit(ctx.consequent));
  }

  @Override
  public UnresolvedExpression visitDataTypeFunctionCall(
      DataTypeFunctionCallContext ctx) {
    return new Cast(visit(ctx.expression()), visit(ctx.convertedDataType()));
  }

  @Override
  public UnresolvedExpression visitConvertedDataType(
      ConvertedDataTypeContext ctx) {
    return AstDSL.stringLiteral(ctx.getText());
  }

  @Override
  public UnresolvedExpression visitRelevanceFunction(RelevanceFunctionContext ctx) {
    return new Function(
        ctx.relevanceFunctionName().getText().toLowerCase(),
        relevanceArguments(ctx));
  }

  private Function visitFunction(String functionName, FunctionArgsContext args) {
    if (args == null) {
      return new Function(functionName, Collections.emptyList());
    }
    return new Function(
        functionName,
        args.functionArg()
            .stream()
            .map(this::visitFunctionArg)
            .collect(Collectors.toList())
    );
  }

  private QualifiedName visitIdentifiers(List<IdentContext> identifiers) {
    return new QualifiedName(
        identifiers.stream()
                   .map(RuleContext::getText)
                   .map(StringUtils::unquoteIdentifier)
                   .collect(Collectors.toList())
    );
  }

  private List<UnresolvedExpression> relevanceArguments(RelevanceFunctionContext ctx) {
    // all the arguments are defaulted to string values
    // to skip environment resolving and function signature resolving
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    builder.add(new UnresolvedArgument("field",
        new Literal(StringUtils.unquoteText(ctx.field.getText()), DataType.STRING)));
    builder.add(new UnresolvedArgument("query",
        new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
    ctx.relevanceArg().forEach(v -> builder.add(new UnresolvedArgument(
        v.relevanceArgName().getText().toLowerCase(), new Literal(StringUtils.unquoteText(
            v.relevanceArgValue().getText()), DataType.STRING))));
    return builder.build();
  }

}
