/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FromClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.HavingClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SelectClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SelectElementContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SubqueryAsRelationContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.TableAsRelationContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.WhereClauseContext;
import static org.opensearch.sql.sql.parser.ParserUtils.getTextInQuery;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QuerySpecificationContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.sql.parser.context.ParsingContext;

/**
 * Abstract syntax tree (AST) builder.
 */
@RequiredArgsConstructor
public class AstBuilder extends OpenSearchSQLParserBaseVisitor<UnresolvedPlan> {

  private final AstExpressionBuilder expressionBuilder = new AstExpressionBuilder();

  /**
   * Parsing context stack that contains context for current query parsing.
   */
  private final ParsingContext context = new ParsingContext();

  /**
   * SQL query to get original token text. This is necessary because token.getText() returns
   * text without whitespaces or other characters discarded by lexer.
   */
  private final String query;

  @Override
  public UnresolvedPlan visitShowStatement(OpenSearchSQLParser.ShowStatementContext ctx) {
    final UnresolvedExpression tableFilter = visitAstExpression(ctx.tableFilter());
    return new Project(Collections.singletonList(AllFields.of()))
        .attach(new Filter(tableFilter).attach(new Relation(qualifiedName(TABLE_INFO))));
  }

  @Override
  public UnresolvedPlan visitDescribeStatement(OpenSearchSQLParser.DescribeStatementContext ctx) {
    final Function tableFilter = (Function) visitAstExpression(ctx.tableFilter());
    final String tableName = tableFilter.getFuncArgs().get(1).toString();
    final Relation table = new Relation(qualifiedName(mappingTable(tableName.toString())));
    if (ctx.columnFilter() == null) {
      return new Project(Collections.singletonList(AllFields.of())).attach(table);
    } else {
      return new Project(Collections.singletonList(AllFields.of()))
          .attach(new Filter(visitAstExpression(ctx.columnFilter())).attach(table));
    }
  }

  @Override
  public UnresolvedPlan visitQuerySpecification(QuerySpecificationContext queryContext) {
    context.push();
    context.peek().collect(queryContext, query);

    Project project = (Project) visit(queryContext.selectClause());

    if (queryContext.fromClause() == null) {
      Optional<UnresolvedExpression> allFields =
          project.getProjectList().stream().filter(node -> node instanceof AllFields)
              .findFirst();
      if (allFields.isPresent()) {
        throw new SyntaxCheckException("No FROM clause found for select all");
      }
      // Attach an Values operator with only a empty row inside so that
      // Project operator can have a chance to evaluate its expression
      // though the evaluation doesn't have any dependency on what's in Values.
      Values emptyValue = new Values(ImmutableList.of(emptyList()));
      return project.attach(emptyValue);
    }

    // If limit (and offset) keyword exists:
    // Add Limit node, plan structure becomes:
    // Project -> Limit -> visit(fromClause)
    // Else:
    // Project -> visit(fromClause)
    UnresolvedPlan from = visit(queryContext.fromClause());
    if (queryContext.limitClause() != null) {
      from = visit(queryContext.limitClause()).attach(from);
    }
    UnresolvedPlan result = project.attach(from);
    context.pop();
    return result;
  }

  @Override
  public UnresolvedPlan visitSelectClause(SelectClauseContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder =
        new ImmutableList.Builder<>();
    if (ctx.selectElements().star != null) { //TODO: project operator should be required?
      builder.add(AllFields.of());
    }
    ctx.selectElements().selectElement().forEach(field -> builder.add(visitSelectItem(field)));
    return new Project(builder.build());
  }

  @Override
  public UnresolvedPlan visitLimitClause(OpenSearchSQLParser.LimitClauseContext ctx) {
    return new Limit(
        Integer.parseInt(ctx.limit.getText()),
        ctx.offset == null ? 0 : Integer.parseInt(ctx.offset.getText())
    );
  }

  @Override
  public UnresolvedPlan visitFromClause(FromClauseContext ctx) {
    UnresolvedPlan result = visit(ctx.relation());

    if (ctx.whereClause() != null) {
      result = visit(ctx.whereClause()).attach(result);
    }

    // Because aggregation maybe implicit, this has to be handled here instead of visitGroupByClause
    AstAggregationBuilder aggBuilder = new AstAggregationBuilder(context.peek());
    UnresolvedPlan aggregation = aggBuilder.visit(ctx.groupByClause());
    if (aggregation != null) {
      result = aggregation.attach(result);
    }

    if (ctx.havingClause() != null) {
      UnresolvedPlan havingPlan = visit(ctx.havingClause());
      verifySupportsCondition(((Filter) havingPlan).getCondition());
      result = visit(ctx.havingClause()).attach(result);
    }

    if (ctx.orderByClause() != null) {
      AstSortBuilder sortBuilder = new AstSortBuilder(context.peek());
      result = sortBuilder.visit(ctx.orderByClause()).attach(result);
    }
    return result;
  }

  /**
   * Ensure NESTED function is not used in HAVING clause and fallback to legacy engine.
   * Can remove when support is added for NESTED function in HAVING clause.
   * @param func : Function in HAVING clause
   */
  private void verifySupportsCondition(UnresolvedExpression func) {
    if (func instanceof Function) {
      if (((Function) func).getFuncName().equalsIgnoreCase(
          BuiltinFunctionName.NESTED.name()
      )) {
        throw new SyntaxCheckException(
            "Falling back to legacy engine. Nested function is not supported in the HAVING clause."
        );
      }
      ((Function)func).getFuncArgs().stream()
          .forEach(e -> verifySupportsCondition(e)
      );
    }
  }

  @Override
  public UnresolvedPlan visitTableAsRelation(TableAsRelationContext ctx) {
    String tableAlias = (ctx.alias() == null) ? null
        : StringUtils.unquoteIdentifier(ctx.alias().getText());
    return new Relation(visitAstExpression(ctx.tableName()), tableAlias);
  }

  @Override
  public UnresolvedPlan visitSubqueryAsRelation(SubqueryAsRelationContext ctx) {
    String subqueryAlias = StringUtils.unquoteIdentifier(ctx.alias().getText());
    return new RelationSubquery(visit(ctx.subquery), subqueryAlias);
  }

  @Override
  public UnresolvedPlan visitWhereClause(WhereClauseContext ctx) {
    return new Filter(visitAstExpression(ctx.expression()));
  }

  @Override
  public UnresolvedPlan visitHavingClause(HavingClauseContext ctx) {
    AstHavingFilterBuilder builder = new AstHavingFilterBuilder(context.peek());
    return new Filter(builder.visit(ctx.expression()));
  }

  @Override
  protected UnresolvedPlan aggregateResult(UnresolvedPlan aggregate, UnresolvedPlan nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  private UnresolvedExpression visitAstExpression(ParseTree tree) {
    return expressionBuilder.visit(tree);
  }

  private UnresolvedExpression visitSelectItem(SelectElementContext ctx) {
    String name = StringUtils.unquoteIdentifier(getTextInQuery(ctx.expression(), query));
    UnresolvedExpression expr = visitAstExpression(ctx.expression());

    if (ctx.alias() == null) {
      return new Alias(name, expr);
    } else {
      String alias = StringUtils.unquoteIdentifier(ctx.alias().getText());
      return new Alias(name, expr, alias);
    }
  }

}
