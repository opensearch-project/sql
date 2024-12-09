/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.visitor;

import static java.util.Collections.singleton;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.AggregateWindowedFunctionContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.AtomTableItemContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.BinaryComparisonPredicateContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.BooleanLiteralContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.ComparisonOperatorContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.ConstantContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.DecimalLiteralContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.FromClauseContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.FullColumnNameContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.FunctionNameBaseContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.InPredicateContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.IsExpressionContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.MathOperatorContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.MinusSelectContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.OuterJoinContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.PredicateContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.RegexpPredicateContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.RootContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.ScalarFunctionCallContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.SelectElementsContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.SelectExpressionElementContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.SelectFunctionElementContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.SimpleTableNameContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.StringLiteralContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.TableAndTypeNameContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.TableSourceBaseContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.TableSourceItemContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.TableSourcesContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.UdfFunctionCallContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.UidContext;
import static org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.UnionSelectContext;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.InnerJoinContext;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.QuerySpecificationContext;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.SelectColumnElementContext;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.SubqueryTableItemContext;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.TableNamePatternContext;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParserBaseVisitor;

/** ANTLR parse tree visitor to drive the analysis process. */
public class AntlrSqlParseTreeVisitor<T extends Reducible>
    extends OpenSearchLegacySqlParserBaseVisitor<T> {

  /** Generic visitor to perform the real action on parse tree */
  private final GenericSqlParseTreeVisitor<T> visitor;

  public AntlrSqlParseTreeVisitor(GenericSqlParseTreeVisitor<T> visitor) {
    this.visitor = visitor;
  }

  @Override
  public T visitRoot(RootContext ctx) {
    visitor.visitRoot();
    return super.visitRoot(ctx);
  }

  @Override
  public T visitUnionSelect(UnionSelectContext ctx) {
    T union = visitor.visitOperator("UNION");
    return reduce(union, asList(ctx.querySpecification(), ctx.unionStatement()));
  }

  @Override
  public T visitMinusSelect(MinusSelectContext ctx) {
    T minus = visitor.visitOperator("MINUS");
    return reduce(minus, asList(ctx.querySpecification(), ctx.minusStatement()));
  }

  @Override
  public T visitInPredicate(InPredicateContext ctx) {
    T in = visitor.visitOperator("IN");
    PredicateContext field = ctx.predicate();
    ParserRuleContext subquery =
        (ctx.selectStatement() != null) ? ctx.selectStatement() : ctx.expressions();
    return reduce(in, Arrays.asList(field, subquery));
  }

  @Override
  public T visitTableSources(TableSourcesContext ctx) {
    if (ctx.tableSource().size() < 2) {
      return super.visitTableSources(ctx);
    }
    T commaJoin = visitor.visitOperator("JOIN");
    return reduce(commaJoin, ctx.tableSource());
  }

  @Override
  public T visitTableSourceBase(TableSourceBaseContext ctx) {
    if (ctx.joinPart().isEmpty()) {
      return super.visitTableSourceBase(ctx);
    }
    T join = visitor.visitOperator("JOIN");
    return reduce(join, asList(ctx.tableSourceItem(), ctx.joinPart()));
  }

  @Override
  public T visitInnerJoin(InnerJoinContext ctx) {
    return visitJoin(ctx.children, ctx.tableSourceItem());
  }

  @Override
  public T visitOuterJoin(OuterJoinContext ctx) {
    return visitJoin(ctx.children, ctx.tableSourceItem());
  }

  /**
   *
   *
   * <pre>
   * Enforce visit order because ANTLR is generic and unaware.
   *
   * Visiting order is:
   *  FROM
   *  => WHERE
   *   => SELECT
   *    => GROUP BY
   *     => HAVING
   *      => ORDER BY
   *       => LIMIT
   *  </pre>
   */
  @Override
  public T visitQuerySpecification(QuerySpecificationContext ctx) {
    visitor.visitQuery();

    // Always visit FROM clause first to define symbols
    FromClauseContext fromClause = ctx.fromClause();
    visit(fromClause.tableSources());

    if (fromClause.whereExpr != null) {
      visit(fromClause.whereExpr);
    }

    // Note visit GROUP BY and HAVING later than SELECT for alias definition
    T result = visitSelectElements(ctx.selectElements());
    fromClause.groupByItem().forEach(this::visit);
    if (fromClause.havingExpr != null) {
      visit(fromClause.havingExpr);
    }

    if (ctx.orderByClause() != null) {
      visitOrderByClause(ctx.orderByClause());
    }
    if (ctx.limitClause() != null) {
      visitLimitClause(ctx.limitClause());
    }

    visitor.endVisitQuery();
    return result;
  }

  @Override
  public T visitSubqueryTableItem(SubqueryTableItemContext ctx) {
    throw new EarlyExitAnalysisException("Exit when meeting subquery in from");
  }

  /** Visit here instead of tableName because we need alias */
  @Override
  public T visitAtomTableItem(AtomTableItemContext ctx) {
    String alias = (ctx.alias == null) ? "" : ctx.alias.getText();
    T result = visit(ctx.tableName());
    visitor.visitAs(alias, result);
    return result;
  }

  @Override
  public T visitSimpleTableName(SimpleTableNameContext ctx) {
    return visitor.visitIndexName(ctx.getText());
  }

  @Override
  public T visitTableNamePattern(TableNamePatternContext ctx) {
    return visitor.visitIndexName(ctx.getText());
  }

  @Override
  public T visitTableAndTypeName(TableAndTypeNameContext ctx) {
    return visitor.visitIndexName(ctx.uid(0).getText());
  }

  @Override
  public T visitFullColumnName(FullColumnNameContext ctx) {
    return visitor.visitFieldName(ctx.getText());
  }

  @Override
  public T visitUdfFunctionCall(UdfFunctionCallContext ctx) {
    String funcName = ctx.fullId().getText();
    T func = visitor.visitFunctionName(funcName);
    return reduce(func, ctx.functionArgs());
  }

  @Override
  public T visitScalarFunctionCall(ScalarFunctionCallContext ctx) {
    UnsupportedSemanticVerifier.verify(ctx);
    T func = visit(ctx.scalarFunctionName());
    return reduce(func, ctx.functionArgs());
  }

  @Override
  public T visitMathOperator(MathOperatorContext ctx) {
    UnsupportedSemanticVerifier.verify(ctx);
    return super.visitMathOperator(ctx);
  }

  @Override
  public T visitRegexpPredicate(RegexpPredicateContext ctx) {
    UnsupportedSemanticVerifier.verify(ctx);
    return super.visitRegexpPredicate(ctx);
  }

  @Override
  public T visitSelectElements(SelectElementsContext ctx) {
    return visitor.visitSelect(
        ctx.selectElement().stream().map(this::visit).collect(Collectors.toList()));
  }

  @Override
  public T visitSelectStarElement(OpenSearchLegacySqlParser.SelectStarElementContext ctx) {
    return visitor.visitSelectAllColumn();
  }

  @Override
  public T visitSelectColumnElement(SelectColumnElementContext ctx) {
    return visitSelectItem(ctx.fullColumnName(), ctx.uid());
  }

  @Override
  public T visitSelectFunctionElement(SelectFunctionElementContext ctx) {
    return visitSelectItem(ctx.functionCall(), ctx.uid());
  }

  @Override
  public T visitSelectExpressionElement(SelectExpressionElementContext ctx) {
    return visitSelectItem(ctx.expression(), ctx.uid());
  }

  @Override
  public T visitAggregateWindowedFunction(AggregateWindowedFunctionContext ctx) {
    String funcName = ctx.getChild(0).getText();
    T func = visitor.visitFunctionName(funcName);
    return reduce(func, ctx.functionArg());
  }

  @Override
  public T visitFunctionNameBase(FunctionNameBaseContext ctx) {
    return visitor.visitFunctionName(ctx.getText());
  }

  @Override
  public T visitBinaryComparisonPredicate(BinaryComparisonPredicateContext ctx) {
    if (isNamedArgument(ctx)) { // Essentially named argument is assign instead of comparison
      return defaultResult();
    }

    T op = visit(ctx.comparisonOperator());
    return reduce(op, Arrays.asList(ctx.left, ctx.right));
  }

  @Override
  public T visitIsExpression(IsExpressionContext ctx) {
    T op = visitor.visitOperator("IS");
    return op.reduce(
        Arrays.asList(visit(ctx.predicate()), visitor.visitBoolean(ctx.testValue.getText())));
  }

  @Override
  public T visitConvertedDataType(OpenSearchLegacySqlParser.ConvertedDataTypeContext ctx) {
    if (ctx.getChild(0) != null && !Strings.isNullOrEmpty(ctx.getChild(0).getText())) {
      return visitor.visitConvertedType(ctx.getChild(0).getText());
    } else {
      return super.visitConvertedDataType(ctx);
    }
  }

  @Override
  public T visitComparisonOperator(ComparisonOperatorContext ctx) {
    return visitor.visitOperator(ctx.getText());
  }

  @Override
  public T visitConstant(ConstantContext ctx) {
    if (ctx.REAL_LITERAL() != null) {
      return visitor.visitFloat(ctx.getText());
    }
    if (ctx.dateType != null) {
      return visitor.visitDate(ctx.getText());
    }
    if (ctx.nullLiteral != null) {
      return visitor.visitNull();
    }
    return super.visitConstant(ctx);
  }

  @Override
  public T visitStringLiteral(StringLiteralContext ctx) {
    return visitor.visitString(ctx.getText());
  }

  @Override
  public T visitDecimalLiteral(DecimalLiteralContext ctx) {
    return visitor.visitInteger(ctx.getText());
  }

  @Override
  public T visitBooleanLiteral(BooleanLiteralContext ctx) {
    return visitor.visitBoolean(ctx.getText());
  }

  @Override
  protected T defaultResult() {
    return visitor.defaultValue();
  }

  @Override
  protected T aggregateResult(T aggregate, T nextResult) {
    if (nextResult != defaultResult()) { // Simply return non-default value for now
      return nextResult;
    }
    return aggregate;
  }

  /**
   * Named argument, ex. TOPHITS('size'=3), is under FunctionArgs -> Predicate And the function name
   * should be contained in openSearchFunctionNameBase
   */
  private boolean isNamedArgument(BinaryComparisonPredicateContext ctx) {
    if (ctx.getParent() != null
        && ctx.getParent().getParent() != null
        && ctx.getParent().getParent().getParent() != null
        && ctx.getParent().getParent().getParent() instanceof ScalarFunctionCallContext) {

      ScalarFunctionCallContext parent =
          (ScalarFunctionCallContext) ctx.getParent().getParent().getParent();
      return parent.scalarFunctionName().functionNameBase().openSearchFunctionNameBase() != null;
    }
    return false;
  }

  /** Enforce visiting result of table instead of ON clause as result */
  private T visitJoin(List<ParseTree> children, TableSourceItemContext tableCtx) {
    T result = defaultResult();
    for (ParseTree child : children) {
      if (child == tableCtx) {
        result = visit(tableCtx);
      } else {
        visit(child);
      }
    }
    return result;
  }

  /** Visit select items for type check and alias definition */
  private T visitSelectItem(ParserRuleContext item, UidContext uid) {
    T result = visit(item);
    if (uid != null) {
      visitor.visitAs(uid.getText(), result);
    }
    return result;
  }

  private T reduce(T reducer, ParserRuleContext ctx) {
    return reduce(reducer, (ctx == null) ? List.of() : ctx.children);
  }

  /** Make constructor apply arguments and return result type */
  private <Node extends ParseTree> T reduce(T reducer, List<Node> nodes) {
    List<T> args;
    if (nodes == null) {
      args = List.of();
    } else {
      args =
          nodes.stream()
              .map(this::visit)
              .filter(type -> type != defaultResult())
              .collect(Collectors.toList());
    }
    return reducer.reduce(args);
  }

  /** Combine an item and a list of items to a single list */
  private <Node1 extends ParseTree, Node2 extends ParseTree> List<ParseTree> asList(
      Node1 first, List<Node2> rest) {

    List<ParseTree> result = new ArrayList<>(singleton(first));
    result.addAll(rest);
    return result;
  }
}
