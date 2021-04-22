/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.sql.parser;

import static com.amazon.opendistroforelasticsearch.sql.ast.dsl.AstDSL.qualifiedName;
import static com.amazon.opendistroforelasticsearch.sql.ast.dsl.AstDSL.stringLiteral;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName.LIKE;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName.NOT_LIKE;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName.REGEXP;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.BinaryComparisonPredicateContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.BooleanContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.CaseFuncAlternativeContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.CaseFunctionCallContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.ColumnFilterContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.ConvertedDataTypeContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.CountStarFunctionCallContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.DataTypeFunctionCallContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.DateLiteralContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.IsNullPredicateContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.LikePredicateContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.MathExpressionAtomContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.NotExpressionContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.NullLiteralContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.OverClauseContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.QualifiedNameContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.RegexpPredicateContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.RegularAggregateFunctionCallContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScalarFunctionCallContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.ScalarWindowFunctionContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.ShowDescribePatternContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.SignedDecimalContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.SignedRealContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.StringContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.StringLiteralContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableFilterContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.TimeLiteralContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.TimestampLiteralContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.WindowFunctionClauseContext;
import static com.amazon.opendistroforelasticsearch.sql.sql.parser.ParserUtils.createSortOption;

import com.amazon.opendistroforelasticsearch.sql.ast.dsl.AstDSL;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.AggregateFunction;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.AllFields;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.And;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.Case;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.Cast;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.Function;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.Interval;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.IntervalUnit;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.Not;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.Or;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.QualifiedName;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.UnresolvedExpression;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.When;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.WindowFunction;
import com.amazon.opendistroforelasticsearch.sql.ast.tree.Sort.SortOption;
import com.amazon.opendistroforelasticsearch.sql.common.utils.StringUtils;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.AndExpressionContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.ColumnNameContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.FunctionArgsContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.IdentContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.IntervalLiteralContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.NestedExpressionAtomContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.OrExpressionContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableNameContext;
import com.amazon.opendistroforelasticsearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.RuleContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

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
    return new AggregateFunction(agg.getFuncName(), agg.getField(), visit(ctx.filterClause()));
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
    return AstDSL.intLiteral(Integer.valueOf(ctx.getText()));
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

}
