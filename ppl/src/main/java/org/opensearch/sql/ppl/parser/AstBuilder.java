/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DescribeCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FromClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.HeadCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RareCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RenameCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchFilterFromContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchFromContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchFromFilterContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TableFunctionContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TableSourceClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TopCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.WhereCommandContext;
import static org.opensearch.sql.utils.SystemIndexUtils.DATASOURCES_TABLE_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.AdCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.ByClauseContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldListContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.KmeansCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ppl.utils.ArgumentFactory;

/** Class of building the AST. Refines the visit path and build the AST nodes */
@RequiredArgsConstructor
public class AstBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedPlan> {

  private final AstExpressionBuilder expressionBuilder;

  /**
   * PPL query to get original token text. This is necessary because token.getText() returns text
   * without whitespaces or other characters discarded by lexer.
   */
  private final String query;

  @Override
  public UnresolvedPlan visitQueryStatement(OpenSearchPPLParser.QueryStatementContext ctx) {
    UnresolvedPlan pplCommand = visit(ctx.pplCommands());
    return ctx.commands().stream().map(this::visit).reduce(pplCommand, (r, e) -> e.attach(r));
  }

  /** Search command. */
  @Override
  public UnresolvedPlan visitSearchFrom(SearchFromContext ctx) {
    return visitFromClause(ctx.fromClause());
  }

  @Override
  public UnresolvedPlan visitSearchFromFilter(SearchFromFilterContext ctx) {
    return new Filter(internalVisitExpression(ctx.logicalExpression()))
        .attach(visit(ctx.fromClause()));
  }

  @Override
  public UnresolvedPlan visitSearchFilterFrom(SearchFilterFromContext ctx) {
    return new Filter(internalVisitExpression(ctx.logicalExpression()))
        .attach(visit(ctx.fromClause()));
  }

  /**
   * Describe command. Current logic separates table and metadata info about table by adding
   * MAPPING_ODFE_SYS_TABLE as suffix. Even with the introduction of datasource and schema name in
   * fully qualified table name, we do the same thing by appending MAPPING_ODFE_SYS_TABLE as syffix
   * to the last part of qualified name.
   */
  @Override
  public UnresolvedPlan visitDescribeCommand(DescribeCommandContext ctx) {
    final Relation table = (Relation) visitTableSourceClause(ctx.tableSourceClause());
    QualifiedName tableQualifiedName = table.getTableQualifiedName();
    ArrayList<String> parts = new ArrayList<>(tableQualifiedName.getParts());
    parts.set(parts.size() - 1, mappingTable(parts.get(parts.size() - 1)));
    return new Relation(new QualifiedName(parts));
  }

  /** Show command. */
  @Override
  public UnresolvedPlan visitShowDataSourcesCommand(
      OpenSearchPPLParser.ShowDataSourcesCommandContext ctx) {
    return new Relation(qualifiedName(DATASOURCES_TABLE_NAME));
  }

  /** Where command. */
  @Override
  public UnresolvedPlan visitWhereCommand(WhereCommandContext ctx) {
    return new Filter(internalVisitExpression(ctx.logicalExpression()));
  }

  /** Fields command. */
  @Override
  public UnresolvedPlan visitFieldsCommand(FieldsCommandContext ctx) {
    return new Project(
        ctx.fieldList().fieldExpression().stream()
            .map(this::internalVisitExpression)
            .collect(Collectors.toList()),
        ArgumentFactory.getArgumentList(ctx));
  }

  /** Rename command. */
  @Override
  public UnresolvedPlan visitRenameCommand(RenameCommandContext ctx) {
    return new Rename(
        ctx.renameClasue().stream()
            .map(
                ct ->
                    new Map(
                        internalVisitExpression(ct.orignalField),
                        internalVisitExpression(ct.renamedField)))
            .collect(Collectors.toList()));
  }

  /** Stats command. */
  @Override
  public UnresolvedPlan visitStatsCommand(StatsCommandContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> aggListBuilder = new ImmutableList.Builder<>();
    for (OpenSearchPPLParser.StatsAggTermContext aggCtx : ctx.statsAggTerm()) {
      UnresolvedExpression aggExpression = internalVisitExpression(aggCtx.statsFunction());
      String name =
          aggCtx.alias == null
              ? getTextInQuery(aggCtx)
              : StringUtils.unquoteIdentifier(aggCtx.alias.getText());
      Alias alias = new Alias(name, aggExpression);
      aggListBuilder.add(alias);
    }

    List<UnresolvedExpression> groupList =
        Optional.ofNullable(ctx.statsByClause())
            .map(OpenSearchPPLParser.StatsByClauseContext::fieldList)
            .map(
                expr ->
                    expr.fieldExpression().stream()
                        .map(
                            groupCtx ->
                                (UnresolvedExpression)
                                    new Alias(
                                        StringUtils.unquoteIdentifier(getTextInQuery(groupCtx)),
                                        internalVisitExpression(groupCtx)))
                        .collect(Collectors.toList()))
            .orElse(Collections.emptyList());

    UnresolvedExpression span =
        Optional.ofNullable(ctx.statsByClause())
            .map(OpenSearchPPLParser.StatsByClauseContext::bySpanClause)
            .map(this::internalVisitExpression)
            .orElse(null);

    Aggregation aggregation =
        new Aggregation(
            aggListBuilder.build(),
            Collections.emptyList(),
            groupList,
            span,
            ArgumentFactory.getArgumentList(ctx));
    return aggregation;
  }

  /** Dedup command. */
  @Override
  public UnresolvedPlan visitDedupCommand(DedupCommandContext ctx) {
    return new Dedupe(ArgumentFactory.getArgumentList(ctx), getFieldList(ctx.fieldList()));
  }

  /** Head command visitor. */
  @Override
  public UnresolvedPlan visitHeadCommand(HeadCommandContext ctx) {
    Integer size = ctx.number != null ? Integer.parseInt(ctx.number.getText()) : 10;
    Integer from = ctx.from != null ? Integer.parseInt(ctx.from.getText()) : 0;
    return new Head(size, from);
  }

  /** Sort command. */
  @Override
  public UnresolvedPlan visitSortCommand(SortCommandContext ctx) {
    return new Sort(
        ctx.sortbyClause().sortField().stream()
            .map(sort -> (Field) internalVisitExpression(sort))
            .collect(Collectors.toList()));
  }

  /** Eval command. */
  @Override
  public UnresolvedPlan visitEvalCommand(EvalCommandContext ctx) {
    return new Eval(
        ctx.evalClause().stream()
            .map(ct -> (Let) internalVisitExpression(ct))
            .collect(Collectors.toList()));
  }

  private List<UnresolvedExpression> getGroupByList(ByClauseContext ctx) {
    return ctx.fieldList().fieldExpression().stream()
        .map(this::internalVisitExpression)
        .collect(Collectors.toList());
  }

  private List<Field> getFieldList(FieldListContext ctx) {
    return ctx.fieldExpression().stream()
        .map(field -> (Field) internalVisitExpression(field))
        .collect(Collectors.toList());
  }

  /** Rare command. */
  @Override
  public UnresolvedPlan visitRareCommand(RareCommandContext ctx) {
    List<UnresolvedExpression> groupList =
        ctx.byClause() == null ? Collections.emptyList() : getGroupByList(ctx.byClause());
    return new RareTopN(
        CommandType.RARE,
        ArgumentFactory.getArgumentList(ctx),
        getFieldList(ctx.fieldList()),
        groupList);
  }

  @Override
  public UnresolvedPlan visitGrokCommand(OpenSearchPPLParser.GrokCommandContext ctx) {
    UnresolvedExpression sourceField = internalVisitExpression(ctx.source_field);
    Literal pattern = (Literal) internalVisitExpression(ctx.pattern);

    return new Parse(ParseMethod.GROK, sourceField, pattern, ImmutableMap.of());
  }

  @Override
  public UnresolvedPlan visitParseCommand(OpenSearchPPLParser.ParseCommandContext ctx) {
    UnresolvedExpression sourceField = internalVisitExpression(ctx.source_field);
    Literal pattern = (Literal) internalVisitExpression(ctx.pattern);

    return new Parse(ParseMethod.REGEX, sourceField, pattern, ImmutableMap.of());
  }

  @Override
  public UnresolvedPlan visitPatternsCommand(OpenSearchPPLParser.PatternsCommandContext ctx) {
    UnresolvedExpression sourceField = internalVisitExpression(ctx.source_field);
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.patternsParameter()
        .forEach(
            x -> {
              builder.put(
                  x.children.get(0).toString(),
                  (Literal) internalVisitExpression(x.children.get(2)));
            });
    java.util.Map<String, Literal> arguments = builder.build();
    Literal pattern = arguments.getOrDefault("pattern", AstDSL.stringLiteral(""));

    return new Parse(ParseMethod.PATTERNS, sourceField, pattern, arguments);
  }

  /** Top command. */
  @Override
  public UnresolvedPlan visitTopCommand(TopCommandContext ctx) {
    List<UnresolvedExpression> groupList =
        ctx.byClause() == null ? Collections.emptyList() : getGroupByList(ctx.byClause());
    return new RareTopN(
        CommandType.TOP,
        ArgumentFactory.getArgumentList(ctx),
        getFieldList(ctx.fieldList()),
        groupList);
  }

  /** From clause. */
  @Override
  public UnresolvedPlan visitFromClause(FromClauseContext ctx) {
    if (ctx.tableFunction() != null) {
      return visitTableFunction(ctx.tableFunction());
    } else {
      return visitTableSourceClause(ctx.tableSourceClause());
    }
  }

  @Override
  public UnresolvedPlan visitTableSourceClause(TableSourceClauseContext ctx) {
    return new Relation(
        ctx.tableSource().stream().map(this::internalVisitExpression).collect(Collectors.toList()));
  }

  @Override
  public UnresolvedPlan visitTableFunction(TableFunctionContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    ctx.functionArgs()
        .functionArg()
        .forEach(
            arg -> {
              String argName = (arg.ident() != null) ? arg.ident().getText() : null;
              builder.add(
                  new UnresolvedArgument(argName, this.internalVisitExpression(arg.expression())));
            });
    return new TableFunction(this.internalVisitExpression(ctx.qualifiedName()), builder.build());
  }

  /** Navigate to & build AST expression. */
  private UnresolvedExpression internalVisitExpression(ParseTree tree) {
    return expressionBuilder.visit(tree);
  }

  /** Simply return non-default value for now. */
  @Override
  protected UnresolvedPlan aggregateResult(UnresolvedPlan aggregate, UnresolvedPlan nextResult) {
    if (nextResult != defaultResult()) {
      return nextResult;
    }
    return aggregate;
  }

  /** Kmeans command. */
  @Override
  public UnresolvedPlan visitKmeansCommand(KmeansCommandContext ctx) {
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.kmeansParameter()
        .forEach(
            x -> {
              builder.put(
                  x.children.get(0).toString(),
                  (Literal) internalVisitExpression(x.children.get(2)));
            });
    return new Kmeans(builder.build());
  }

  /** AD command. */
  @Override
  public UnresolvedPlan visitAdCommand(AdCommandContext ctx) {
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.adParameter()
        .forEach(
            x -> {
              builder.put(
                  x.children.get(0).toString(),
                  (Literal) internalVisitExpression(x.children.get(2)));
            });

    return new AD(builder.build());
  }

  /** ml command. */
  @Override
  public UnresolvedPlan visitMlCommand(OpenSearchPPLParser.MlCommandContext ctx) {
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.mlArg()
        .forEach(
            x -> {
              builder.put(x.argName.getText(), (Literal) internalVisitExpression(x.argValue));
            });
    return new ML(builder.build());
  }

  /** fillnull command. */
  @Override
  public UnresolvedPlan visitFillNullWithTheSameValue(
      OpenSearchPPLParser.FillNullWithTheSameValueContext ctx) {
    return new FillNull(
        FillNull.ContainNullableFieldFill.ofSameValue(
            internalVisitExpression(ctx.nullReplacement),
            ctx.nullableFieldList.fieldExpression().stream()
                .map(f -> (Field) internalVisitExpression(f))
                .toList()));
  }

  /** fillnull command. */
  @Override
  public UnresolvedPlan visitFillNullWithFieldVariousValues(
      OpenSearchPPLParser.FillNullWithFieldVariousValuesContext ctx) {
    ImmutableList.Builder<FillNull.NullableFieldFill> replacementsBuilder = ImmutableList.builder();
    for (int i = 0; i < ctx.nullReplacementExpression().size(); i++) {
      replacementsBuilder.add(
          new FillNull.NullableFieldFill(
              (Field) internalVisitExpression(ctx.nullReplacementExpression(i).nullableField),
              internalVisitExpression(ctx.nullReplacementExpression(i).nullReplacement)));
    }

    return new FillNull(
        FillNull.ContainNullableFieldFill.ofVariousValue(replacementsBuilder.build()));
  }

  /** Get original text in query. */
  private String getTextInQuery(ParserRuleContext ctx) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop();
    return query.substring(start.getStartIndex(), stop.getStopIndex() + 1);
  }
}
