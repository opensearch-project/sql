/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FromClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.HeadCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.PplStatementContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RareCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RenameCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchFilterFromContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchFromContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchFromFilterContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TopCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.WhereCommandContext;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.ByClauseContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldListContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ppl.utils.ArgumentFactory;

/**
 * Class of building the AST.
 * Refines the visit path and build the AST nodes
 */
@RequiredArgsConstructor
public class AstBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedPlan> {

  private final AstExpressionBuilder expressionBuilder;

  /**
   * PPL query to get original token text. This is necessary because token.getText() returns
   * text without whitespaces or other characters discarded by lexer.
   */
  private final String query;

  @Override
  public UnresolvedPlan visitPplStatement(PplStatementContext ctx) {
    UnresolvedPlan search = visit(ctx.searchCommand());
    return ctx.commands()
        .stream()
        .map(this::visit)
        .reduce(search, (r, e) -> e.attach(r));
  }

  /**
   * Search command.
   */
  @Override
  public UnresolvedPlan visitSearchFrom(SearchFromContext ctx) {
    return visitFromClause(ctx.fromClause());
  }

  @Override
  public UnresolvedPlan visitSearchFromFilter(SearchFromFilterContext ctx) {
    return new Filter(visitExpression(ctx.logicalExpression())).attach(visit(ctx.fromClause()));
  }

  @Override
  public UnresolvedPlan visitSearchFilterFrom(SearchFilterFromContext ctx) {
    return new Filter(visitExpression(ctx.logicalExpression())).attach(visit(ctx.fromClause()));
  }

  /**
   * Where command.
   */
  @Override
  public UnresolvedPlan visitWhereCommand(WhereCommandContext ctx) {
    return new Filter(visitExpression(ctx.logicalExpression()));
  }

  /**
   * Fields command.
   */
  @Override
  public UnresolvedPlan visitFieldsCommand(FieldsCommandContext ctx) {
    return new Project(
        ctx.fieldList()
            .fieldExpression()
            .stream()
            .map(this::visitExpression)
            .collect(Collectors.toList()),
        ArgumentFactory.getArgumentList(ctx)
    );
  }

  /**
   * Rename command.
   */
  @Override
  public UnresolvedPlan visitRenameCommand(RenameCommandContext ctx) {
    return new Rename(
        ctx.renameClasue()
            .stream()
            .map(ct -> new Map(visitExpression(ct.orignalField), visitExpression(ct.renamedField)))
            .collect(Collectors.toList())
    );
  }

  /**
   * Stats command.
   */
  @Override
  public UnresolvedPlan visitStatsCommand(StatsCommandContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> aggListBuilder = new ImmutableList.Builder<>();
    for (OpenSearchPPLParser.StatsAggTermContext aggCtx : ctx.statsAggTerm()) {
      UnresolvedExpression aggExpression = visitExpression(aggCtx.statsFunction());
      String name = aggCtx.alias == null ? getTextInQuery(aggCtx) : StringUtils
          .unquoteIdentifier(aggCtx.alias.getText());
      Alias alias = new Alias(name, aggExpression);
      aggListBuilder.add(alias);
    }

    List<UnresolvedExpression> groupList =
        Optional.ofNullable(ctx.statsByClause())
            .map(OpenSearchPPLParser.StatsByClauseContext::fieldList)
            .map(expr -> expr.fieldExpression().stream()
                        .map(groupCtx ->
                            (UnresolvedExpression) new Alias(getTextInQuery(groupCtx),
                                visitExpression(groupCtx)))
                        .collect(Collectors.toList()))
            .orElse(Collections.emptyList());

    UnresolvedExpression span =
        Optional.ofNullable(ctx.statsByClause())
            .map(OpenSearchPPLParser.StatsByClauseContext::bySpanClause)
            .map(this::visitExpression)
            .orElse(null);

    Aggregation aggregation = new Aggregation(
        aggListBuilder.build(),
        Collections.emptyList(),
        groupList,
        span,
        ArgumentFactory.getArgumentList(ctx)
    );
    return aggregation;
  }

  /**
   * Dedup command.
   */
  @Override
  public UnresolvedPlan visitDedupCommand(DedupCommandContext ctx) {
    return new Dedupe(
        ArgumentFactory.getArgumentList(ctx),
        getFieldList(ctx.fieldList())
    );
  }

  /**
   * Head command visitor.
   */
  @Override
  public UnresolvedPlan visitHeadCommand(HeadCommandContext ctx) {
    Integer size = ctx.number != null ? Integer.parseInt(ctx.number.getText()) : 10;
    return new Head(size);
  }

  /**
   * Sort command.
   */
  @Override
  public UnresolvedPlan visitSortCommand(SortCommandContext ctx) {
    return new Sort(
        ctx.sortbyClause()
            .sortField()
            .stream()
            .map(sort -> (Field) visitExpression(sort))
            .collect(Collectors.toList())
    );
  }

  /**
   * Eval command.
   */
  @Override
  public UnresolvedPlan visitEvalCommand(EvalCommandContext ctx) {
    return new Eval(
        ctx.evalClause()
            .stream()
            .map(ct -> (Let) visitExpression(ct))
            .collect(Collectors.toList())
    );
  }

  private List<UnresolvedExpression> getGroupByList(ByClauseContext ctx) {
    return ctx.fieldList().fieldExpression().stream().map(this::visitExpression)
        .collect(Collectors.toList());
  }

  private List<Field> getFieldList(FieldListContext ctx) {
    return ctx.fieldExpression()
        .stream()
        .map(field -> (Field) visitExpression(field))
        .collect(Collectors.toList());
  }

  /**
   * Rare command.
   */
  @Override
  public UnresolvedPlan visitRareCommand(RareCommandContext ctx) {
    List<UnresolvedExpression> groupList = ctx.byClause() == null ? Collections.emptyList() :
        getGroupByList(ctx.byClause());
    return new RareTopN(
        CommandType.RARE,
        ArgumentFactory.getArgumentList(ctx),
        getFieldList(ctx.fieldList()),
        groupList
    );
  }

  /**
   * Top command.
   */
  @Override
  public UnresolvedPlan visitTopCommand(TopCommandContext ctx) {
    List<UnresolvedExpression> groupList = ctx.byClause() == null ? Collections.emptyList() :
        getGroupByList(ctx.byClause());
    return new RareTopN(
        CommandType.TOP,
        ArgumentFactory.getArgumentList(ctx),
        getFieldList(ctx.fieldList()),
        groupList
    );
  }

  /**
   * From clause.
   */
  @Override
  public UnresolvedPlan visitFromClause(FromClauseContext ctx) {
    return new Relation(ctx.tableSource()
        .stream().map(this::visitExpression)
        .collect(Collectors.toList()));
  }

  /**
   * Navigate to & build AST expression.
   */
  private UnresolvedExpression visitExpression(ParseTree tree) {
    return expressionBuilder.visit(tree);
  }

  /**
   * Simply return non-default value for now.
   */
  @Override
  protected UnresolvedPlan aggregateResult(UnresolvedPlan aggregate, UnresolvedPlan nextResult) {
    if (nextResult != defaultResult()) {
      return nextResult;
    }
    return aggregate;
  }

  /**
   * Get original text in query.
   */
  private String getTextInQuery(ParserRuleContext ctx) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop();
    return query.substring(start.getStartIndex(), stop.getStopIndex() + 1);
  }
}
