/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.calcite.utils.CalciteUtils.getOnlyForCalciteException;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BinCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DescribeCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DynamicSourceClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.EvalCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.HeadCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RenameCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchFromContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TableCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TableFunctionContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TableSourceClauseContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.WhereCommandContext;
import static org.opensearch.sql.utils.SystemIndexUtils.DATASOURCES_TABLE_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.EmptySourcePropagateVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Argument.ArgumentMap;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.PatternMethod;
import org.opensearch.sql.ast.expression.PatternMode;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.SearchAnd;
import org.opensearch.sql.ast.expression.SearchExpression;
import org.opensearch.sql.ast.expression.SearchGroup;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFrame;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.AddColTotals;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Chart;
import org.opensearch.sql.ast.tree.CountBin;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.DefaultBin;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.MinSpanBin;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RangeBin;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Regex;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.ReplacePair;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SpanBin;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Transpose;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.AdCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.ByClauseContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldListContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IdentsAsQualifiedNameSeqContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.KmeansCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.LookupPairContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsByClauseContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ppl.utils.ArgumentFactory;
import org.opensearch.sql.ppl.utils.UnresolvedPlanHelper;

/** Class of building the AST. Refines the visit path and build the AST nodes */
public class AstBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedPlan> {

  private final AstExpressionBuilder expressionBuilder;

  private final Settings settings;

  /**
   * PPL query to get original token text. This is necessary because token.getText() returns text
   * without whitespaces or other characters discarded by lexer.
   */
  private final String query;

  public AstBuilder(String query) {
    this(query, null);
  }

  public AstBuilder(String query, Settings settings) {
    this.expressionBuilder = new AstExpressionBuilder(this);
    this.query = query;
    this.settings = settings;
  }

  public Settings getSettings() {
    return settings;
  }

  @Override
  public UnresolvedPlan visitQueryStatement(OpenSearchPPLParser.QueryStatementContext ctx) {
    UnresolvedPlan pplCommand = visit(ctx.pplCommands());
    return ctx.commands().stream()
        .map(this::visit)
        .reduce(pplCommand, (r, e) -> e.attach(e instanceof Join ? projectExceptMeta(r) : r));
  }

  @Override
  public UnresolvedPlan visitSubPipeline(OpenSearchPPLParser.SubPipelineContext ctx) {
    List<OpenSearchPPLParser.CommandsContext> cmds = ctx.commands();
    if (cmds.isEmpty()) {
      throw new IllegalArgumentException("appendpipe [] is empty");
    }
    UnresolvedPlan seed = visit(cmds.get(0));
    return cmds.stream().skip(1).map(this::visit).reduce(seed, (left, op) -> op.attach(left));
  }

  @Override
  public UnresolvedPlan visitSubSearch(OpenSearchPPLParser.SubSearchContext ctx) {
    UnresolvedPlan searchCommand = visit(ctx.searchCommand());
    // Exclude metadata fields for subquery
    return projectExceptMeta(
        ctx.commands().stream().map(this::visit).reduce(searchCommand, (r, e) -> e.attach(r)));
  }

  /** Search command. */
  @Override
  public UnresolvedPlan visitSearchFrom(SearchFromContext ctx) {
    if (ctx.searchExpression().isEmpty()) {
      return visitFromClause(ctx.fromClause());
    } else {
      // Build search expressions using visitor pattern
      List<SearchExpression> searchExprs =
          ctx.searchExpression().stream()
              .map(expr -> (SearchExpression) expressionBuilder.visit(expr))
              .collect(Collectors.toList());
      // Combine multiple expressions with AND
      SearchExpression combined;
      if (searchExprs.size() == 1) {
        combined = searchExprs.get(0);
      } else {
        // before being combined with AND (e.g., "a=1 b=-1" becomes "(a:1) AND (b:-1)")
        combined =
            searchExprs.stream()
                .map(SearchGroup::new)
                .map(SearchExpression.class::cast)
                .reduce(SearchAnd::new)
                .get(); // Safe because we know size > 1 from the if condition
      }

      // Convert to query string
      String queryString = combined.toQueryString();

      // Create Search node with relation and query string
      Relation relation = (Relation) visitFromClause(ctx.fromClause());
      return new Search(relation, queryString, combined);
    }
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
    parts.set(parts.size() - 1, mappingTable(parts.get(parts.size() - 1), PPL_SPEC));
    return new DescribeRelation(new QualifiedName(parts));
  }

  /** Show command. */
  @Override
  public UnresolvedPlan visitShowDataSourcesCommand(
      OpenSearchPPLParser.ShowDataSourcesCommandContext ctx) {
    return new DescribeRelation(qualifiedName(DATASOURCES_TABLE_NAME));
  }

  /** Where command. */
  @Override
  public UnresolvedPlan visitWhereCommand(WhereCommandContext ctx) {
    return new Filter(internalVisitExpression(ctx.logicalExpression()));
  }

  @Override
  public UnresolvedPlan visitAppendPipeCommand(OpenSearchPPLParser.AppendPipeCommandContext ctx) {
    UnresolvedPlan plan = visit(ctx.subPipeline());
    return new AppendPipe(plan);
  }

  @Override
  public UnresolvedPlan visitJoinCommand(OpenSearchPPLParser.JoinCommandContext ctx) {
    // a sql-like syntax if join criteria existed
    boolean sqlLike = ctx.joinCriteria() != null;
    Join.JoinType joinType = null;
    if (sqlLike) {
      joinType = ArgumentFactory.getJoinType(ctx.sqlLikeJoinType());
    }
    List<Argument> arguments =
        ctx.joinOption().stream()
            .map(o -> (Argument) expressionBuilder.visit(o))
            .collect(Collectors.toList());
    if (arguments.stream().noneMatch(arg -> arg.getArgName().equals("max"))
        && !UnresolvedPlanHelper.legacyPreferred(settings)) {
      arguments = new ArrayList<>(arguments);
      arguments.add(new Argument("max", Literal.ONE));
    }
    Argument.ArgumentMap argumentMap = Argument.ArgumentMap.of(arguments);
    if (argumentMap.get("type") != null) {
      Join.JoinType joinTypeFromArgument = ArgumentFactory.getJoinType(argumentMap);
      if (sqlLike && joinType != joinTypeFromArgument && ctx.sqlLikeJoinType() != null) {
        throw new SemanticCheckException(
            "Join type is ambiguous, remove either the join type before JOIN keyword or 'type='"
                + " option.");
      }
      joinType = joinTypeFromArgument;
    }
    if (!sqlLike && argumentMap.get("type") == null) {
      joinType = Join.JoinType.INNER;
    }
    validateJoinType(joinType);

    Join.JoinHint joinHint = getJoinHint(ctx.joinHintList());
    Optional<String> leftAlias = Optional.empty();
    Optional<String> rightAlias = Optional.empty();
    if (ctx.sideAlias() != null && ctx.sideAlias().leftAlias != null) {
      leftAlias = Optional.of(internalVisitExpression(ctx.sideAlias().leftAlias).toString());
    }
    if (ctx.tableOrSubqueryClause().alias != null) {
      rightAlias =
          Optional.of(internalVisitExpression(ctx.tableOrSubqueryClause().alias).toString());
    }
    if (ctx.sideAlias() != null && ctx.sideAlias().rightAlias != null) {
      rightAlias = Optional.of(internalVisitExpression(ctx.sideAlias().rightAlias).toString());
    }

    UnresolvedPlan rightRelation = visit(ctx.tableOrSubqueryClause());
    // Add a SubqueryAlias to the right plan when the right alias is present and no duplicated alias
    // existing in right.
    UnresolvedPlan right;
    if (rightAlias.isEmpty()
        || (rightRelation instanceof SubqueryAlias
            && rightAlias.get().equals(((SubqueryAlias) rightRelation).getAlias()))) {
      right = rightRelation;
    } else {
      right = new SubqueryAlias(rightAlias.get(), rightRelation);
    }
    Optional<UnresolvedExpression> joinCondition =
        ctx.joinCriteria() == null
            ? Optional.empty()
            : Optional.of(expressionBuilder.visitJoinCriteria(ctx.joinCriteria()));
    Optional<List<Field>> joinFields = Optional.empty();
    if (ctx.fieldList() != null) {
      joinFields = Optional.of(getFieldList(ctx.fieldList()));
    }
    return new Join(
        projectExceptMeta(right),
        leftAlias,
        rightAlias,
        joinType,
        joinCondition,
        joinHint,
        joinFields,
        argumentMap);
  }

  private Join.JoinHint getJoinHint(OpenSearchPPLParser.JoinHintListContext ctx) {
    Join.JoinHint joinHint;
    if (ctx == null) {
      joinHint = new Join.JoinHint();
    } else {
      joinHint =
          new Join.JoinHint(
              ctx.hintPair().stream()
                  .map(expressionBuilder::visit)
                  .filter(e -> e instanceof EqualTo)
                  .map(e -> (EqualTo) e)
                  .collect(
                      Collectors.toMap(
                          k -> k.getLeft().toString(), // always literal
                          v -> v.getRight().toString(), // always literal
                          (v1, v2) -> v2,
                          LinkedHashMap::new)));
    }
    return joinHint;
  }

  private void validateJoinType(Join.JoinType joinType) {
    Object config = settings.getSettingValue(Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);
    if (config != null && !((Boolean) config)) {
      if (Join.highCostJoinTypes().contains(joinType)) {
        throw new SemanticCheckException(
            String.format(
                "Join type %s is performance sensitive. Set %s to true to enable it.",
                joinType.name(), Key.CALCITE_SUPPORT_ALL_JOIN_TYPES.getKeyValue()));
      }
    }
  }

  @Override
  public UnresolvedPlan visitFieldsCommand(FieldsCommandContext ctx) {
    return buildProjectCommand(ctx.fieldsCommandBody(), ArgumentFactory.getArgumentList(ctx));
  }

  /** Table command as an alias for fields command. */
  @Override
  public UnresolvedPlan visitTableCommand(TableCommandContext ctx) {
    if (settings != null
        && Boolean.TRUE.equals(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED))) {
      // Table command uses the same structure as fields command
      List<Argument> arguments =
          Collections.singletonList(
              ctx.fieldsCommandBody().MINUS() != null
                  ? new Argument("exclude", new Literal(true, DataType.BOOLEAN))
                  : new Argument("exclude", new Literal(false, DataType.BOOLEAN)));
      return buildProjectCommand(ctx.fieldsCommandBody(), arguments);
    }
    throw getOnlyForCalciteException("Table command");
  }

  private UnresolvedPlan buildProjectCommand(
      OpenSearchPPLParser.FieldsCommandBodyContext bodyCtx, List<Argument> arguments) {
    List<UnresolvedExpression> fields = extractFieldExpressions(bodyCtx);

    // Check for enhanced field features when Calcite is explicitly disabled
    if (settings != null
        && Boolean.FALSE.equals(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED))) {
      if (hasEnhancedFieldFeatures(bodyCtx, fields)) {
        throw getOnlyForCalciteException("Enhanced fields feature");
      }
    }

    return new Project(fields, arguments);
  }

  private List<UnresolvedExpression> extractFieldExpressions(
      OpenSearchPPLParser.FieldsCommandBodyContext bodyCtx) {
    if (bodyCtx.wcFieldList() != null) {
      return processFieldExpressions(bodyCtx.wcFieldList().selectFieldExpression());
    }
    return Collections.emptyList();
  }

  private List<UnresolvedExpression> processFieldExpressions(
      List<OpenSearchPPLParser.SelectFieldExpressionContext> fieldExpressions) {
    var stream = fieldExpressions.stream().map(this::internalVisitExpression);

    if (settings != null
        && Boolean.TRUE.equals(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED))) {
      stream = stream.distinct();
    }

    return stream.collect(Collectors.toList());
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

  /** Replace command. */
  @Override
  public UnresolvedPlan visitReplaceCommand(OpenSearchPPLParser.ReplaceCommandContext ctx) {
    // Parse all replacement pairs
    List<ReplacePair> replacePairs =
        ctx.replacePair().stream().map(this::buildReplacePair).collect(Collectors.toList());

    Set<Field> fieldList = getUniqueFieldSet(ctx.fieldList());

    return new Replace(replacePairs, fieldList);
  }

  /** Build a ReplacePair from parse context. */
  private ReplacePair buildReplacePair(OpenSearchPPLParser.ReplacePairContext ctx) {
    Literal pattern = (Literal) internalVisitExpression(ctx.pattern);
    Literal replacement = (Literal) internalVisitExpression(ctx.replacement);
    return new ReplacePair(pattern, replacement);
  }

  /** Stats command. */
  @Override
  public UnresolvedPlan visitStatsCommand(StatsCommandContext ctx) {
    List<UnresolvedExpression> aggregations = parseAggTerms(ctx.statsAggTerm());

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
            .orElse(emptyList());

    UnresolvedExpression span =
        Optional.ofNullable(ctx.statsByClause())
            .map(OpenSearchPPLParser.StatsByClauseContext::bySpanClause)
            .map(this::internalVisitExpression)
            .orElse(null);

    Aggregation aggregation =
        new Aggregation(
            aggregations,
            Collections.emptyList(),
            groupList,
            span,
            ArgumentFactory.getArgumentList(ctx, settings));
    return aggregation;
  }

  /** Eventstats command. */
  public UnresolvedPlan visitEventstatsCommand(OpenSearchPPLParser.EventstatsCommandContext ctx) {
    // 1. Parse arguments from the eventstats command
    List<Argument> argExprList = ArgumentFactory.getArgumentList(ctx, settings);
    ArgumentMap arguments = ArgumentMap.of(argExprList);

    // bucket_nullable
    boolean bucketNullable = (Boolean) arguments.get(Argument.BUCKET_NULLABLE).getValue();

    // 2. Build groupList
    List<UnresolvedExpression> groupList = getPartitionExprList(ctx.statsByClause());

    ImmutableList.Builder<UnresolvedExpression> windownFunctionListBuilder =
        new ImmutableList.Builder<>();
    for (OpenSearchPPLParser.EventstatsAggTermContext aggCtx : ctx.eventstatsAggTerm()) {
      UnresolvedExpression windowFunction = internalVisitExpression(aggCtx.windowFunction());
      // set partition by list for window function
      if (windowFunction instanceof WindowFunction) {
        ((WindowFunction) windowFunction).setPartitionByList(groupList);
      }
      String name =
          aggCtx.alias == null
              ? getTextInQuery(aggCtx)
              : StringUtils.unquoteIdentifier(aggCtx.alias.getText());
      Alias alias = new Alias(name, windowFunction);
      windownFunctionListBuilder.add(alias);
    }

    return new Window(windownFunctionListBuilder.build(), groupList, bucketNullable);
  }

  /** Streamstats command. */
  public UnresolvedPlan visitStreamstatsCommand(OpenSearchPPLParser.StreamstatsCommandContext ctx) {
    // 1. Parse arguments from the streamstats command
    List<Argument> argExprList = ArgumentFactory.getArgumentList(ctx, settings);
    ArgumentMap arguments = ArgumentMap.of(argExprList);

    // current, window, global and bucket_nullable from ArgumentFactory
    boolean current = (Boolean) arguments.get("current").getValue();
    int window = (Integer) arguments.get("window").getValue();
    boolean global = (Boolean) arguments.get("global").getValue();
    boolean bucketNullable = (Boolean) arguments.get(Argument.BUCKET_NULLABLE).getValue();

    if (window < 0) {
      throw new IllegalArgumentException("Window size must be >= 0, but got: " + window);
    }

    // reset_before, reset_after
    UnresolvedExpression resetBeforeExpr =
        Optional.ofNullable(ctx.streamstatsArgs())
            .filter(args -> args.resetBeforeArg() != null && !args.resetBeforeArg().isEmpty())
            .map(args -> expressionBuilder.visit(args.resetBeforeArg(0).logicalExpression()))
            .orElse(null);

    UnresolvedExpression resetAfterExpr =
        Optional.ofNullable(ctx.streamstatsArgs())
            .filter(args -> args.resetAfterArg() != null && !args.resetAfterArg().isEmpty())
            .map(args -> expressionBuilder.visit(args.resetAfterArg(0).logicalExpression()))
            .orElse(null);

    // 2.1 Build a WindowFrame from the provided arguments
    WindowFrame frame = buildFrameFromArgs(current, window);
    // 2.2 Build groupList
    List<UnresolvedExpression> groupList = getPartitionExprList(ctx.statsByClause());

    // 3. Build each window function in the command
    ImmutableList.Builder<UnresolvedExpression> windowFunctionListBuilder =
        new ImmutableList.Builder<>();

    for (OpenSearchPPLParser.StreamstatsAggTermContext aggCtx : ctx.streamstatsAggTerm()) {
      UnresolvedExpression windowFunction = internalVisitExpression(aggCtx.windowFunction());
      if (windowFunction instanceof WindowFunction) {
        WindowFunction wf = (WindowFunction) windowFunction;
        // Attach PARTITION BY clause expressions
        wf.setPartitionByList(groupList);
        // Inject the frame
        wf.setWindowFrame(frame);
      }
      String name =
          aggCtx.alias == null
              ? getTextInQuery(aggCtx)
              : StringUtils.unquoteIdentifier(aggCtx.alias.getText());
      Alias alias = new Alias(name, windowFunction);
      windowFunctionListBuilder.add(alias);
    }

    // 4. Build StreamWindow AST node
    return new StreamWindow(
        windowFunctionListBuilder.build(),
        groupList,
        current,
        window,
        global,
        bucketNullable,
        resetBeforeExpr,
        resetAfterExpr);
  }

  private WindowFrame buildFrameFromArgs(boolean current, int window) {
    // Build the frame
    if (window > 0) {
      if (current) {
        // N-1 PRECEDING to CURRENT ROW
        return WindowFrame.of(
            WindowFrame.FrameType.ROWS, (window - 1) + " PRECEDING", "CURRENT ROW");
      } else {
        // N PRECEDING to 1 PRECEDING
        return WindowFrame.of(WindowFrame.FrameType.ROWS, window + " PRECEDING", "1 PRECEDING");
      }
    } else {
      // Default: running total
      if (current) {
        return WindowFrame.toCurrentRow();
      } else {
        // Default: running total excluding current row
        return WindowFrame.of(WindowFrame.FrameType.ROWS, "UNBOUNDED PRECEDING", "1 PRECEDING");
      }
    }
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

  /** Bin command visitor. */
  @Override
  public UnresolvedPlan visitBinCommand(BinCommandContext ctx) {
    UnresolvedExpression field = internalVisitExpression(ctx.fieldExpression());

    // Handle alias from binCommand context
    String alias = ctx.alias != null ? StringUtils.unquoteIdentifier(ctx.alias.getText()) : null;

    // Track seen parameters for duplicate detection
    Set<String> seenParams = new HashSet<>();

    // Initialize all optional parameters
    UnresolvedExpression span = null;
    Integer bins = null;
    UnresolvedExpression minspan = null;
    UnresolvedExpression aligntime = null;
    UnresolvedExpression start = null;
    UnresolvedExpression end = null;
    String errorFormat = "Duplicate %s parameter in bin command";
    // Process each bin option: detect duplicates and assign values in one shot
    for (OpenSearchPPLParser.BinOptionContext option : ctx.binOption()) {
      UnresolvedExpression resolvedOption = internalVisitExpression(option);
      // SPAN parameter
      if (option.span != null) {
        checkParamDuplication(seenParams, option.SPAN(), errorFormat);
        span = resolvedOption;
      }
      // BINS parameter
      if (option.bins != null) {
        checkParamDuplication(seenParams, option.BINS(), errorFormat);
        bins = (Integer) ((Literal) resolvedOption).getValue();
      }
      // MINSPAN parameter
      if (option.minspan != null) {
        checkParamDuplication(seenParams, option.MINSPAN(), errorFormat);
        minspan = resolvedOption;
      }
      // ALIGNTIME parameter
      if (option.aligntime != null) {
        checkParamDuplication(seenParams, option.ALIGNTIME(), errorFormat);
        aligntime = resolvedOption;
      }
      // START parameter
      if (option.start != null) {
        checkParamDuplication(seenParams, option.START(), errorFormat);
        start = resolvedOption;
      }
      // END parameter
      if (option.end != null) {
        checkParamDuplication(seenParams, option.END(), errorFormat);
        end = resolvedOption;
      }
    }

    // Create appropriate Bin subclass based on priority order (matches AstDSL.bin() logic)
    if (span != null) {
      // 1. SPAN (highest priority) -> SpanBin
      return SpanBin.builder().field(field).span(span).aligntime(aligntime).alias(alias).build();
    } else if (minspan != null) {
      // 2. MINSPAN (second priority) -> MinSpanBin
      return MinSpanBin.builder()
          .field(field)
          .minspan(minspan)
          .start(start)
          .end(end)
          .alias(alias)
          .build();
    } else if (bins != null) {
      // 3. BINS (third priority) -> CountBin
      return CountBin.builder().field(field).bins(bins).start(start).end(end).alias(alias).build();
    } else if (start != null || end != null) {
      // 4. START/END only (fourth priority) -> RangeBin
      return RangeBin.builder().field(field).start(start).end(end).alias(alias).build();
    } else {
      // 5. No parameters (default) -> DefaultBin
      return DefaultBin.builder().field(field).alias(alias).build();
    }
  }

  private void checkParamDuplication(
      Set<String> seenParams, TerminalNode terminalNode, String errorFormat) {
    String paramName = terminalNode.getText();
    if (!seenParams.add(paramName)) {
      throw new IllegalArgumentException(StringUtils.format(errorFormat, paramName));
    }
  }

  /** Sort command. */
  @Override
  public UnresolvedPlan visitSortCommand(SortCommandContext ctx) {
    Integer count = ctx.count != null ? Math.max(0, Integer.parseInt(ctx.count.getText())) : 0;

    List<OpenSearchPPLParser.SortFieldContext> sortFieldContexts = ctx.sortbyClause().sortField();
    validateSortDirectionSyntax(sortFieldContexts);

    List<Field> sortFields =
        sortFieldContexts.stream()
            .map(sort -> (Field) internalVisitExpression(sort))
            .collect(Collectors.toList());

    return new Sort(count, sortFields);
  }

  private void validateSortDirectionSyntax(List<OpenSearchPPLParser.SortFieldContext> sortFields) {
    boolean hasPrefix =
        sortFields.stream()
            .anyMatch(sortField -> sortField instanceof OpenSearchPPLParser.PrefixSortFieldContext);
    boolean hasSuffix =
        sortFields.stream()
            .anyMatch(sortField -> sortField instanceof OpenSearchPPLParser.SuffixSortFieldContext);

    if (hasPrefix && hasSuffix) {
      throw new SemanticCheckException(
          "Cannot mix prefix (+/-) and suffix (asc/desc) sort direction syntax in the same"
              + " command.");
    }
  }

  /** Reverse command. */
  @Override
  public UnresolvedPlan visitReverseCommand(OpenSearchPPLParser.ReverseCommandContext ctx) {
    return new Reverse();
  }

  /** Transpose command. */
  @Override
  public UnresolvedPlan visitTransposeCommand(OpenSearchPPLParser.TransposeCommandContext ctx) {
    java.util.Map<String, Argument> arguments = ArgumentFactory.getArgumentList(ctx);
    return new Transpose(arguments);
  }

  /** Chart command. */
  @Override
  public UnresolvedPlan visitChartCommand(OpenSearchPPLParser.ChartCommandContext ctx) {
    UnresolvedExpression rowSplit =
        ctx.rowSplit() == null ? null : internalVisitExpression(ctx.rowSplit());
    UnresolvedExpression columnSplit =
        ctx.columnSplit() == null ? null : internalVisitExpression(ctx.columnSplit());
    List<Argument> arguments = ArgumentFactory.getArgumentList(ctx);
    UnresolvedExpression aggFunction = parseAggTerms(List.of(ctx.statsAggTerm())).get(0);
    return Chart.builder()
        .rowSplit(rowSplit)
        .columnSplit(columnSplit)
        .aggregationFunction(aggFunction)
        .arguments(arguments)
        .build();
  }

  private List<UnresolvedExpression> parseAggTerms(
      List<OpenSearchPPLParser.StatsAggTermContext> statsAggTermContexts) {
    ImmutableList.Builder<UnresolvedExpression> aggListBuilder = new ImmutableList.Builder<>();
    for (OpenSearchPPLParser.StatsAggTermContext aggCtx : statsAggTermContexts) {
      UnresolvedExpression aggExpression = internalVisitExpression(aggCtx.statsFunction());
      String name =
          aggCtx.alias == null
              ? getTextInQuery(aggCtx)
              : StringUtils.unquoteIdentifier(aggCtx.alias.getText());
      Alias alias = new Alias(name, aggExpression);
      aggListBuilder.add(alias);
    }
    return aggListBuilder.build();
  }

  /** Timechart command. */
  @Override
  public UnresolvedPlan visitTimechartCommand(OpenSearchPPLParser.TimechartCommandContext ctx) {
    List<Argument> arguments = ArgumentFactory.getArgumentList(ctx, expressionBuilder);
    ArgumentMap argMap = ArgumentMap.of(arguments);
    Literal spanLiteral = argMap.getOrDefault("spanliteral", AstDSL.stringLiteral("1m"));
    String timeFieldName =
        Optional.ofNullable(argMap.get("timefield"))
            .map(l -> (String) l.getValue())
            .orElse(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP);
    Field spanField = AstDSL.field(timeFieldName);
    Alias span =
        AstDSL.alias(timeFieldName, AstDSL.spanFromSpanLengthLiteral(spanField, spanLiteral));
    UnresolvedExpression aggregateFunction = parseAggTerms(List.of(ctx.statsAggTerm())).get(0);
    UnresolvedExpression byField =
        Optional.ofNullable(ctx.fieldExpression())
            .map(
                f ->
                    AstDSL.alias(
                        StringUtils.unquoteIdentifier(getTextInQuery(f)),
                        internalVisitExpression(f)))
            .orElse(null);
    return Chart.builder()
        .aggregationFunction(aggregateFunction)
        .rowSplit(span)
        .columnSplit(byField)
        .arguments(arguments)
        .build();
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

  private Set<Field> getUniqueFieldSet(FieldListContext ctx) {
    List<Field> fields =
        ctx.fieldExpression().stream()
            .map(field -> (Field) internalVisitExpression(field))
            .collect(Collectors.toList());

    Set<Field> uniqueFields = new java.util.LinkedHashSet<>(fields);

    if (uniqueFields.size() < fields.size()) {
      // Find duplicates for error message
      Set<String> seen = new HashSet<>();
      Set<String> duplicates =
          fields.stream()
              .map(f -> f.getField().toString())
              .filter(name -> !seen.add(name))
              .collect(Collectors.toSet());

      throw new IllegalArgumentException(
          String.format("Duplicate fields [%s] in Replace command", String.join(", ", duplicates)));
    }

    return uniqueFields;
  }

  /** Rare and Top commands. */
  @Override
  public UnresolvedPlan visitRareTopCommand(OpenSearchPPLParser.RareTopCommandContext ctx) {
    List<UnresolvedExpression> groupList =
        ctx.byClause() == null ? emptyList() : getGroupByList(ctx.byClause());
    Integer noOfResults =
        ctx.number != null
            ? (Integer) ((Literal) expressionBuilder.visitIntegerLiteral(ctx.number)).getValue()
            : 10;
    return new RareTopN(
        ctx.TOP() != null ? CommandType.TOP : CommandType.RARE,
        noOfResults,
        ArgumentFactory.getArgumentList(ctx, settings),
        getFieldList(ctx.fieldList()),
        groupList);
  }

  /** expand command. */
  @Override
  public UnresolvedPlan visitExpandCommand(OpenSearchPPLParser.ExpandCommandContext ctx) {
    Field fieldExpression = (Field) internalVisitExpression(ctx.fieldExpression());
    String alias = ctx.alias != null ? internalVisitExpression(ctx.alias).toString() : null;
    return new Expand(fieldExpression, alias);
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
  public UnresolvedPlan visitSpathCommand(OpenSearchPPLParser.SpathCommandContext ctx) {
    String inField = null;
    String outField = null;
    String path = null;

    for (OpenSearchPPLParser.SpathParameterContext param : ctx.spathParameter()) {
      if (param.input != null) {
        inField = param.input.getText();
      }
      if (param.output != null) {
        outField = param.output.getText();
      }
      if (param.path != null) {
        path = param.path.getText();
      }
    }

    if (inField == null) {
      throw new IllegalArgumentException("`input` parameter is required for `spath`");
    }

    if (outField != null && path == null) {
      throw new IllegalArgumentException(
          "`path` parameter is required for `spath` when `output` is specified");
    }

    return new SPath(inField, outField, path);
  }

  @Override
  public UnresolvedPlan visitPatternsCommand(OpenSearchPPLParser.PatternsCommandContext ctx) {
    UnresolvedExpression sourceField = internalVisitExpression(ctx.source_field);
    ImmutableMap.Builder<String, Literal> builder = ImmutableMap.builder();
    ctx.patternsParameter()
        .forEach(
            x -> {
              String argName = x.children.get(0).toString();
              Literal value = (Literal) internalVisitExpression(x.children.get(2));
              builder.put(argName, value);
            });
    java.util.Map<String, Literal> arguments = builder.build();

    ImmutableMap.Builder<String, Literal> cmdOptionsBuilder = ImmutableMap.builder();
    ctx.patternsCommandOption()
        .forEach(
            option -> {
              String argName = option.children.get(0).toString();
              Literal value = (Literal) internalVisitExpression(option.children.get(2));
              cmdOptionsBuilder.put(argName, value);
            });
    java.util.Map<String, Literal> cmdOptions = cmdOptionsBuilder.build();
    String patternMethod =
        cmdOptions
            .getOrDefault(
                "method", AstDSL.stringLiteral(settings.getSettingValue(Key.PATTERN_METHOD)))
            .toString();
    String patternMode =
        cmdOptions
            .getOrDefault("mode", AstDSL.stringLiteral(settings.getSettingValue(Key.PATTERN_MODE)))
            .toString();
    Literal patternMaxSampleCount =
        cmdOptions.getOrDefault(
            "max_sample_count",
            AstDSL.intLiteral(settings.getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT)));
    Literal patternBufferLimit =
        cmdOptions.getOrDefault(
            "buffer_limit", AstDSL.intLiteral(settings.getSettingValue(Key.PATTERN_BUFFER_LIMIT)));
    Literal showNumberedToken =
        cmdOptions.getOrDefault(
            "show_numbered_token",
            AstDSL.booleanLiteral(settings.getSettingValue(Key.PATTERN_SHOW_NUMBERED_TOKEN)));
    List<UnresolvedExpression> partitionByList = getPartitionExprList(ctx.statsByClause());

    return new Patterns(
        sourceField,
        partitionByList,
        arguments.getOrDefault("new_field", AstDSL.stringLiteral("patterns_field")).toString(),
        PatternMethod.valueOf(patternMethod.toUpperCase(Locale.ROOT)),
        PatternMode.valueOf(patternMode.toUpperCase(Locale.ROOT)),
        patternMaxSampleCount,
        patternBufferLimit,
        showNumberedToken,
        arguments);
  }

  /** Lookup command */
  @Override
  public UnresolvedPlan visitLookupCommand(OpenSearchPPLParser.LookupCommandContext ctx) {
    Relation lookupRelation = new Relation(this.internalVisitExpression(ctx.tableSource()));
    Lookup.OutputStrategy strategy =
        ctx.APPEND() != null ? Lookup.OutputStrategy.APPEND : Lookup.OutputStrategy.REPLACE;
    java.util.Map<String, String> mappingAliasMap =
        buildFieldAliasMap(ctx.lookupMappingList().lookupPair());
    java.util.Map<String, String> outputAliasMap =
        ctx.outputCandidateList() == null
            ? emptyMap()
            : buildFieldAliasMap(ctx.outputCandidateList().lookupPair());
    return new Lookup(lookupRelation, mappingAliasMap, strategy, outputAliasMap);
  }

  private java.util.Map<String, String> buildFieldAliasMap(
      List<LookupPairContext> lookupPairContext) {
    return lookupPairContext.stream()
        .collect(
            Collectors.toMap(
                pair -> pair.inputField.getText(),
                pair -> pair.AS() != null ? pair.outputField.getText() : pair.inputField.getText(),
                (x, y) -> y,
                LinkedHashMap::new));
  }

  @Override
  public UnresolvedPlan visitTableOrSubqueryClause(
      OpenSearchPPLParser.TableOrSubqueryClauseContext ctx) {
    if (ctx.subSearch() != null) {
      return ctx.alias != null
          ? new SubqueryAlias(
              internalVisitExpression(ctx.alias).toString(), visitSubSearch(ctx.subSearch()))
          : visitSubSearch(ctx.subSearch());
    } else {
      return visitTableSourceClause(ctx.tableSourceClause());
    }
  }

  @Override
  public UnresolvedPlan visitTableSourceClause(TableSourceClauseContext ctx) {
    Relation relation =
        new Relation(
            ctx.tableSource().stream()
                .map(this::internalVisitExpression)
                .collect(Collectors.toList()));
    return ctx.alias != null
        ? new SubqueryAlias(internalVisitExpression(ctx.alias).toString(), relation)
        : relation;
  }

  @Override
  public UnresolvedPlan visitDynamicSourceClause(DynamicSourceClauseContext ctx) {
    throw new UnsupportedOperationException(
        "Dynamic source clause with metadata filters is not supported.");
  }

  @Override
  public UnresolvedPlan visitTableFunction(TableFunctionContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
    ctx.namedFunctionArgs()
        .namedFunctionArg()
        .forEach(
            arg -> {
              String argName = (arg.ident() != null) ? arg.ident().getText() : null;
              builder.add(
                  new UnresolvedArgument(
                      argName, this.internalVisitExpression(arg.functionArgExpression())));
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
  public UnresolvedPlan visitFillNullWith(OpenSearchPPLParser.FillNullWithContext ctx) {
    if (ctx.IN() != null) {
      return FillNull.ofSameValue(
          internalVisitExpression(ctx.replacement),
          ctx.fieldList().fieldExpression().stream()
              .map(f -> (Field) internalVisitExpression(f))
              .collect(Collectors.toList()));
    } else {
      return FillNull.ofSameValue(internalVisitExpression(ctx.replacement), List.of());
    }
  }

  /** fillnull command. */
  @Override
  public UnresolvedPlan visitFillNullUsing(OpenSearchPPLParser.FillNullUsingContext ctx) {
    ImmutableList.Builder<Pair<Field, UnresolvedExpression>> replacementsBuilder =
        ImmutableList.builder();
    for (int i = 0; i < ctx.replacementPair().size(); i++) {
      replacementsBuilder.add(
          Pair.of(
              (Field) internalVisitExpression(ctx.replacementPair(i).fieldExpression()),
              internalVisitExpression(ctx.replacementPair(i).replacement)));
    }

    return FillNull.ofVariousValue(replacementsBuilder.build());
  }

  /** fillnull command - value= syntax: fillnull value=<expr> field1 field2 ... */
  @Override
  public UnresolvedPlan visitFillNullValueWithFields(
      OpenSearchPPLParser.FillNullValueWithFieldsContext ctx) {
    return FillNull.ofSameValue(
        internalVisitExpression(ctx.replacement),
        ctx.fieldList().fieldExpression().stream()
            .map(f -> (Field) internalVisitExpression(f))
            .collect(Collectors.toList()),
        true);
  }

  /** fillnull command - value= syntax: fillnull value=<expr> */
  @Override
  public UnresolvedPlan visitFillNullValueAllFields(
      OpenSearchPPLParser.FillNullValueAllFieldsContext ctx) {
    return FillNull.ofSameValue(internalVisitExpression(ctx.replacement), List.of(), true);
  }

  @Override
  public UnresolvedPlan visitFlattenCommand(OpenSearchPPLParser.FlattenCommandContext ctx) {
    Field field = (Field) internalVisitExpression(ctx.fieldExpression());
    List<String> aliases =
        ctx.aliases == null ? null : getAliasList((IdentsAsQualifiedNameSeqContext) ctx.aliases);
    return new Flatten(field, aliases);
  }

  private List<String> getAliasList(IdentsAsQualifiedNameSeqContext ctx) {
    return ctx.qualifiedName().stream()
        .map(this::internalVisitExpression)
        .map(Object::toString)
        .collect(Collectors.toList());
  }

  /** trendline command. */
  @Override
  public UnresolvedPlan visitTrendlineCommand(OpenSearchPPLParser.TrendlineCommandContext ctx) {
    List<Trendline.TrendlineComputation> trendlineComputations =
        ctx.trendlineClause().stream()
            .map(expressionBuilder::visit)
            .map(Trendline.TrendlineComputation.class::cast)
            .collect(Collectors.toList());
    return Optional.ofNullable(ctx.sortField())
        .map(this::internalVisitExpression)
        .map(Field.class::cast)
        .map(sort -> new Trendline(Optional.of(sort), trendlineComputations))
        .orElse(new Trendline(Optional.empty(), trendlineComputations));
  }

  @Override
  public UnresolvedPlan visitAppendcolCommand(OpenSearchPPLParser.AppendcolCommandContext ctx) {
    final Optional<UnresolvedPlan> subsearch =
        ctx.commands().stream().map(this::visit).reduce((r, e) -> e.attach(r));
    final boolean override = (ctx.override != null && Boolean.parseBoolean(ctx.override.getText()));
    if (subsearch.isEmpty()) {
      throw new SemanticCheckException("subsearch should not be empty");
    }
    return new AppendCol(override, subsearch.get());
  }

  @Override
  public UnresolvedPlan visitRegexCommand(OpenSearchPPLParser.RegexCommandContext ctx) {
    UnresolvedExpression field = internalVisitExpression(ctx.regexExpr().field);
    boolean negated = ctx.regexExpr().operator.getType() == OpenSearchPPLParser.NOT_EQUAL;
    Literal pattern = (Literal) internalVisitExpression(ctx.regexExpr().pattern);

    return new Regex(field, negated, pattern);
  }

  @Override
  public UnresolvedPlan visitAppendCommand(OpenSearchPPLParser.AppendCommandContext ctx) {
    UnresolvedPlan searchCommandInSubSearch =
        ctx.searchCommand() != null
            ? visit(ctx.searchCommand())
            : EmptySourcePropagateVisitor
                .EMPTY_SOURCE; // Represents 0 row * 0 col empty input syntax
    UnresolvedPlan subsearch =
        ctx.commands().stream()
            .map(this::visit)
            .reduce(searchCommandInSubSearch, (r, e) -> e.attach(r));

    return new Append(subsearch);
  }

  @Override
  public UnresolvedPlan visitMultisearchCommand(OpenSearchPPLParser.MultisearchCommandContext ctx) {
    List<UnresolvedPlan> subsearches = new ArrayList<>();

    // Process each subsearch
    for (OpenSearchPPLParser.SubSearchContext subsearchCtx : ctx.subSearch()) {
      // Use the existing visitSubSearch logic
      UnresolvedPlan fullSubsearch = visitSubSearch(subsearchCtx);
      subsearches.add(fullSubsearch);
    }

    // Validate minimum number of subsearches
    if (subsearches.size() < 2) {
      throw new SyntaxCheckException(
          "Multisearch command requires at least two subsearches. Provided: " + subsearches.size());
    }

    return new Multisearch(subsearches);
  }

  @Override
  public UnresolvedPlan visitRexCommand(OpenSearchPPLParser.RexCommandContext ctx) {
    UnresolvedExpression field = internalVisitExpression(ctx.rexExpr().field);
    Literal pattern = (Literal) internalVisitExpression(ctx.rexExpr().pattern);
    Rex.RexMode mode = Rex.RexMode.EXTRACT;
    Optional<Integer> maxMatch = Optional.empty();
    Optional<String> offsetField = Optional.empty();

    for (OpenSearchPPLParser.RexOptionContext optionCtx : ctx.rexExpr().rexOption()) {
      if (optionCtx.maxMatch != null) {
        maxMatch = Optional.of(Integer.parseInt(optionCtx.maxMatch.getText()));
      }
      if (optionCtx.EXTRACT() != null) {
        mode = Rex.RexMode.EXTRACT;
      }
      if (optionCtx.SED() != null) {
        mode = Rex.RexMode.SED;
      }
      if (optionCtx.offsetField != null) {
        offsetField = Optional.of(optionCtx.offsetField.getText());
      }
    }

    if (mode == Rex.RexMode.SED && offsetField.isPresent()) {
      throw new IllegalArgumentException(
          "Rex command: offset_field cannot be used with mode=sed. "
              + "The offset_field option is only supported in extract mode.");
    }

    int maxMatchLimit =
        (settings != null) ? settings.getSettingValue(Settings.Key.PPL_REX_MAX_MATCH_LIMIT) : 10;

    int userMaxMatch = maxMatch.orElse(1);
    int effectiveMaxMatch;

    if (userMaxMatch == 0) {
      effectiveMaxMatch = maxMatchLimit;
    } else if (userMaxMatch > maxMatchLimit) {
      throw new IllegalArgumentException(
          String.format(
              "Rex command max_match value (%d) exceeds the configured limit (%d). "
                  + "Consider using a smaller max_match value"
                  + (settings != null
                      ? " or adjust the plugins.ppl.rex.max_match.limit setting."
                      : "."),
              userMaxMatch,
              maxMatchLimit));
    } else {
      effectiveMaxMatch = userMaxMatch;
    }

    return new Rex(field, pattern, mode, Optional.of(effectiveMaxMatch), offsetField);
  }

  /** Get original text in query. */
  private String getTextInQuery(ParserRuleContext ctx) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop();
    return query.substring(start.getStartIndex(), stop.getStopIndex() + 1);
  }

  /**
   * Try to wrap the plan with a project node of this AllFields expression. Only wrap it if the plan
   * is not a project node or if the project is type of excluded.
   *
   * @param plan The input plan needs to be wrapped with a project
   * @return The wrapped plan of the input plan, i.e., project(plan)
   */
  private UnresolvedPlan projectExceptMeta(UnresolvedPlan plan) {
    if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
      return plan;
    } else if (plan instanceof SubqueryAlias) {
      SubqueryAlias subqueryAlias = (SubqueryAlias) plan;
      // don't wrap subquery alias with project, wrap its child
      return new SubqueryAlias(
          subqueryAlias.getAlias(),
          new Project(ImmutableList.of(AllFieldsExcludeMeta.of()))
              .attach(subqueryAlias.getChild().get(0)));
    } else {
      return new Project(ImmutableList.of(AllFieldsExcludeMeta.of())).attach(plan);
    }
  }

  /** Get partition by expression list or group by expression list. */
  private List<UnresolvedExpression> getPartitionExprList(StatsByClauseContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> partExprListBuilder = new ImmutableList.Builder<>();
    Optional.ofNullable(ctx)
        .map(OpenSearchPPLParser.StatsByClauseContext::bySpanClause)
        .map(this::internalVisitExpression)
        .ifPresent(partExprListBuilder::add);

    Optional.ofNullable(ctx)
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
        .ifPresent(partExprListBuilder::addAll);
    return partExprListBuilder.build();
  }

  private boolean hasEnhancedFieldFeatures(
      OpenSearchPPLParser.FieldsCommandBodyContext bodyCtx, List<UnresolvedExpression> fields) {
    if (hasActualWildcards(bodyCtx)) {
      return true;
    }

    return hasSpaceDelimitedFields(bodyCtx);
  }

  private boolean hasSpaceDelimitedFields(OpenSearchPPLParser.FieldsCommandBodyContext bodyCtx) {
    if (bodyCtx.wcFieldList() == null) {
      return false;
    }

    String fieldsText = getTextInQuery(bodyCtx.wcFieldList());

    // If all fields are backtick-enclosed (like eval expressions), don't treat as enhanced
    if (isAllFieldsBacktickEnclosed(bodyCtx)) {
      return false;
    }

    if (bodyCtx.wcFieldList().selectFieldExpression().size() > 1 && !fieldsText.contains(",")) {
      return true;
    }

    if (fieldsText.contains(",") && hasSpacesBetweenFields(fieldsText)) {
      return true;
    }

    return false;
  }

  private boolean hasSpacesBetweenFields(String fieldsText) {
    String[] parts = fieldsText.split(",");
    for (String part : parts) {
      String trimmed = part.trim();
      if (trimmed.contains(" ") && trimmed.split("\\s+").length > 1) {
        // If the field is backtick-enclosed, it's likely an eval expression, not space-delimited
        if (!trimmed.startsWith("`") || !trimmed.endsWith("`")) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isAllFieldsBacktickEnclosed(
      OpenSearchPPLParser.FieldsCommandBodyContext bodyCtx) {
    for (var fieldExpr : bodyCtx.wcFieldList().selectFieldExpression()) {
      if (fieldExpr.wcQualifiedName() != null) {
        String originalText = getTextInQuery(fieldExpr.wcQualifiedName());
        if (!originalText.startsWith("`") || !originalText.endsWith("`")) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean hasActualWildcards(OpenSearchPPLParser.FieldsCommandBodyContext bodyCtx) {
    if (bodyCtx.wcFieldList() == null) {
      return false;
    }

    for (var fieldExpr : bodyCtx.wcFieldList().selectFieldExpression()) {
      if (fieldExpr.STAR() != null) {
        return true;
      }

      if (fieldExpr.wcQualifiedName() != null) {
        String originalText = getTextInQuery(fieldExpr.wcQualifiedName());
        if (originalText.contains("*") && !originalText.contains("`")) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public UnresolvedPlan visitAddtotalsCommand(OpenSearchPPLParser.AddtotalsCommandContext ctx) {

    List<Field> fieldList = new ArrayList<>();
    if (ctx.fieldList() != null) {
      fieldList = getFieldList(ctx.fieldList());
    }
    ImmutableMap.Builder<String, Literal> cmdOptionsBuilder = ImmutableMap.builder();
    ctx.addtotalsOption()
        .forEach(
            option -> {
              String argName = option.children.get(0).toString();
              Literal value = (Literal) internalVisitExpression(option.children.get(2));
              cmdOptionsBuilder.put(argName, value);
            });
    java.util.Map<String, Literal> options = cmdOptionsBuilder.build();
    return new AddTotals(fieldList, options);
  }

  @Override
  public UnresolvedPlan visitAddcoltotalsCommand(
      OpenSearchPPLParser.AddcoltotalsCommandContext ctx) {

    List<Field> fieldList = new ArrayList<>();
    if (ctx.fieldList() != null) {
      fieldList = getFieldList(ctx.fieldList());
    }
    ImmutableMap.Builder<String, Literal> cmdOptionsBuilder = ImmutableMap.builder();
    ctx.addcoltotalsOption()
        .forEach(
            option -> {
              String argName = option.children.get(0).toString();
              Literal value = (Literal) internalVisitExpression(option.children.get(2));
              cmdOptionsBuilder.put(argName, value);
            });
    java.util.Map<String, Literal> options = cmdOptionsBuilder.build();
    return new AddColTotals(fieldList, options);
  }
}
