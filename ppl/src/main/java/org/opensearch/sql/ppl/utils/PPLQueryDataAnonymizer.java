/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.opensearch.sql.calcite.utils.PlanUtils.getRelation;
import static org.opensearch.sql.calcite.utils.PlanUtils.transformPlanToAttachChild;
import static org.opensearch.sql.utils.QueryStringUtils.MASK_COLUMN;
import static org.opensearch.sql.utils.QueryStringUtils.MASK_LITERAL;
import static org.opensearch.sql.utils.QueryStringUtils.MASK_TIMESTAMP_COLUMN;
import static org.opensearch.sql.utils.QueryStringUtils.maskField;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Argument.ArgumentMap;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.AddColTotals;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Bin;
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
import org.opensearch.sql.ast.tree.GraphLookup;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.MinSpanBin;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RangeBin;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Regex;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
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
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRemove;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.logical.LogicalSort;

/** Utility class to mask sensitive information in incoming PPL queries. */
public class PPLQueryDataAnonymizer extends AbstractNodeVisitor<String, String> {

  public static final String MASK_TABLE = "table";

  private final AnonymizerExpressionAnalyzer expressionAnalyzer;
  @Getter private final Settings settings;

  public PPLQueryDataAnonymizer(Settings settings) {
    this.expressionAnalyzer = new AnonymizerExpressionAnalyzer(this);
    this.settings = settings;
  }

  /**
   * This method is used to anonymize sensitive data in PPL query. Sensitive data includes user
   * data.
   *
   * @return ppl query string with all user data replace with "***"
   */
  public String anonymizeData(UnresolvedPlan plan) {
    return plan.accept(this, null);
  }

  public String anonymizeStatement(Statement plan) {
    return plan.accept(this, null);
  }

  /** Handle Query Statement. */
  @Override
  public String visitQuery(Query node, String context) {
    return node.getPlan().accept(this, null);
  }

  @Override
  public String visitExplain(Explain node, String context) {
    return StringUtils.format(
        "explain %s %s",
        node.getMode().name().toLowerCase(Locale.ROOT), node.getStatement().accept(this, null));
  }

  @Override
  public String visitRelation(Relation node, String context) {
    if (node instanceof DescribeRelation) {
      return StringUtils.format("describe %s", MASK_TABLE);
    }
    return StringUtils.format("source=%s", MASK_TABLE);
  }

  @Override
  public String visitJoin(Join node, String context) {
    String left = node.getLeft().accept(this, context);
    String rightTableOrSubquery = node.getRight().accept(this, context);
    String right =
        rightTableOrSubquery.startsWith("source=")
            ? rightTableOrSubquery.substring("source=".length())
            : rightTableOrSubquery;
    Argument.ArgumentMap argumentMap = node.getArgumentMap();
    String max =
        argumentMap.get("max") == null
            ? "0"
            : argumentMap.get("max").toString().toLowerCase(Locale.ROOT);
    if (node.getJoinCondition().isEmpty()) {
      String joinType =
          argumentMap.get("type") == null
              ? "inner"
              : argumentMap.get("type").toString().toLowerCase(Locale.ROOT);
      String overwrite =
          argumentMap.get("overwrite") == null
              ? "true"
              : argumentMap.get("overwrite").toString().toLowerCase(Locale.ROOT);
      String fieldList =
          node.getJoinFields().isEmpty()
              ? ""
              : String.join(
                  ",",
                  node.getJoinFields().get().stream()
                      .map(c -> expressionAnalyzer.analyze(c, context))
                      .toList());
      return StringUtils.format(
          "%s | join type=%s overwrite=%s max=%s %s %s",
          left, joinType, MASK_LITERAL, MASK_LITERAL, fieldList, right);
    } else {
      String joinType = node.getJoinType().name().toLowerCase(Locale.ROOT);
      String leftAlias = node.getLeftAlias().map(l -> " left = " + MASK_COLUMN).orElse("");
      String rightAlias = node.getRightAlias().map(r -> " right = " + MASK_COLUMN).orElse("");
      String condition =
          node.getJoinCondition().map(c -> expressionAnalyzer.analyze(c, context)).orElse("true");
      return StringUtils.format(
          "%s | %s join max=%s%s%s on %s %s",
          left, joinType, MASK_LITERAL, leftAlias, rightAlias, condition, right);
    }
  }

  @Override
  public String visitLookup(Lookup node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String mappingFields = formatFieldAlias(node.getMappingAliasMap());
    String strategy =
        node.getOutputAliasMap().isEmpty()
            ? ""
            : String.format(" %s ", node.getOutputStrategy().toString().toLowerCase());
    String outputFields = formatFieldAlias(node.getOutputAliasMap());
    return StringUtils.format(
        "%s | lookup %s %s%s%s", child, MASK_TABLE, mappingFields, strategy, outputFields);
  }

  @Override
  public String visitGraphLookup(GraphLookup node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    StringBuilder command = new StringBuilder();
    command.append(child).append(" | graphlookup ").append(MASK_TABLE);
    if (node.getStartField() != null) {
      command.append(" startField=").append(MASK_COLUMN);
    }
    command.append(" fromField=").append(MASK_COLUMN);
    command.append(" toField=").append(MASK_COLUMN);
    if (node.getMaxDepth() != null && !Integer.valueOf(0).equals(node.getMaxDepth().getValue())) {
      command.append(" maxDepth=").append(MASK_LITERAL);
    }
    if (node.getDepthField() != null) {
      command.append(" depthField=").append(MASK_COLUMN);
    }
    command.append(" direction=").append(node.getDirection().name().toLowerCase());
    command.append(" as ").append(MASK_COLUMN);
    return command.toString();
  }

  private String formatFieldAlias(java.util.Map<String, String> fieldMap) {
    return fieldMap.entrySet().stream()
        .map(
            entry ->
                Objects.equals(entry.getKey(), entry.getValue())
                    ? entry.getKey()
                    : StringUtils.format("%s as %s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(", "));
  }

  @Override
  public String visitSubqueryAlias(SubqueryAlias node, String context) {
    Node childNode = node.getChild().get(0);
    String child = childNode.accept(this, context);
    if (childNode instanceof Project project
        && project.getProjectList().get(0) instanceof AllFields) {
      childNode = childNode.getChild().get(0);
    }
    // add "[]" only if its child is not a root
    String format = childNode.getChild().isEmpty() ? "%s as %s" : "[ %s ] as %s";
    return StringUtils.format(format, child, MASK_COLUMN);
  }

  @Override
  public String visitTableFunction(TableFunction node, String context) {
    String arguments =
        node.getArguments().stream()
            .map(
                unresolvedExpression ->
                    this.expressionAnalyzer.analyze(unresolvedExpression, context))
            .collect(Collectors.joining(","));
    return StringUtils.format("source=%s(%s)", node.getFunctionName().toString(), arguments);
  }

  @Override
  public String visitSearch(Search node, String context) {
    String source = node.getChild().get(0).accept(this, context);
    return StringUtils.format("%s %s", source, node.getOriginalExpression().toAnonymizedString());
  }

  @Override
  public String visitFilter(Filter node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String condition = visitExpression(node.getCondition());
    return StringUtils.format("%s | where %s", child, condition);
  }

  /** Build {@link LogicalRename}. */
  @Override
  public String visitRename(Rename node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    ImmutableMap.Builder<String, String> renameMapBuilder = new ImmutableMap.Builder<>();
    for (Map renameMap : node.getRenameList()) {
      renameMapBuilder.put(
          visitExpression(renameMap.getOrigin()),
          ((Field) renameMap.getTarget()).getField().toString());
    }
    String renames =
        node.getRenameList().stream()
            .map(entry -> StringUtils.format("%s as %s", MASK_COLUMN, MASK_COLUMN))
            .collect(Collectors.joining(","));
    return StringUtils.format("%s | rename %s", child, renames);
  }

  @Override
  public String visitReplace(Replace node, String context) {
    // Get the child query string
    String child = node.getChild().get(0).accept(this, context);

    // Build pattern/replacement pairs string
    String pairs =
        node.getReplacePairs().stream()
            .map(
                pair ->
                    StringUtils.format(
                        "%s WITH %s",
                        visitExpression(pair.getPattern()), visitExpression(pair.getReplacement())))
            .collect(Collectors.joining(", "));

    // Get field list
    String fieldListStr =
        " IN "
            + node.getFieldList().stream().map(Field::toString).collect(Collectors.joining(", "));

    // Build the replace command string
    return StringUtils.format("%s | replace %s%s", child, pairs, fieldListStr);
  }

  /** Build {@link LogicalAggregation}. */
  @Override
  public String visitAggregation(Aggregation node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    UnresolvedExpression span = node.getSpan();
    List<UnresolvedExpression> groupByExprList = new ArrayList<>();
    if (!Objects.isNull(span)) {
      groupByExprList.add(span);
    }
    groupByExprList.addAll(node.getGroupExprList());
    final String group = visitExpressionList(groupByExprList);
    return StringUtils.format(
        "%s | stats %s",
        child, String.join(" ", visitExpressionList(node.getAggExprList()), groupBy(group)).trim());
  }

  @Override
  public String visitBin(Bin node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    StringBuilder binCommand = new StringBuilder();
    binCommand.append(" | bin ").append(visitExpression(node.getField()));

    // Use instanceof for type-safe dispatch to access subclass-specific properties
    if (node instanceof SpanBin) {
      SpanBin spanBin = (SpanBin) node;
      binCommand.append(" span=").append(visitExpression(spanBin.getSpan()));
      if (spanBin.getAligntime() != null) {
        binCommand.append(" aligntime=").append(visitExpression(spanBin.getAligntime()));
      }
    } else if (node instanceof MinSpanBin) {
      MinSpanBin minSpanBin = (MinSpanBin) node;
      binCommand.append(" minspan=").append(visitExpression(minSpanBin.getMinspan()));
      if (minSpanBin.getStart() != null) {
        binCommand.append(" start=").append(visitExpression(minSpanBin.getStart()));
      }
      if (minSpanBin.getEnd() != null) {
        binCommand.append(" end=").append(visitExpression(minSpanBin.getEnd()));
      }
    } else if (node instanceof CountBin) {
      CountBin countBin = (CountBin) node;
      binCommand.append(" bins=").append(MASK_LITERAL);
      if (countBin.getStart() != null) {
        binCommand.append(" start=").append(visitExpression(countBin.getStart()));
      }
      if (countBin.getEnd() != null) {
        binCommand.append(" end=").append(visitExpression(countBin.getEnd()));
      }
    } else if (node instanceof RangeBin) {
      RangeBin rangeBin = (RangeBin) node;
      if (rangeBin.getStart() != null) {
        binCommand.append(" start=").append(visitExpression(rangeBin.getStart()));
      }
      if (rangeBin.getEnd() != null) {
        binCommand.append(" end=").append(visitExpression(rangeBin.getEnd()));
      }
    } else if (node instanceof DefaultBin) {
      // DefaultBin has no additional parameters
    }

    if (node.getAlias() != null) {
      binCommand.append(" as ").append(MASK_COLUMN);
    }

    return StringUtils.format("%s%s", child, binCommand.toString());
  }

  @Override
  public String visitWindow(Window node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    return StringUtils.format(
        "%s | eventstats %s",
        child, String.join(" ", visitExpressionList(node.getWindowFunctionList())).trim());
  }

  @Override
  public String visitStreamWindow(StreamWindow node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    return StringUtils.format(
        "%s | streamstats %s",
        child, String.join(" ", visitExpressionList(node.getWindowFunctionList())).trim());
  }

  /** Build {@link LogicalRareTopN}. */
  @Override
  public String visitRareTopN(RareTopN node, String context) {
    final String child = node.getChild().get(0).accept(this, context);
    ArgumentMap arguments = ArgumentMap.of(node.getArguments());
    Integer noOfResults = node.getNoOfResults();
    String countField = (String) arguments.get(RareTopN.Option.countField.name()).getValue();
    Boolean showCount = (Boolean) arguments.get(RareTopN.Option.showCount.name()).getValue();
    Boolean useNull = (Boolean) arguments.get(RareTopN.Option.useNull.name()).getValue();
    String fields = visitFieldList(node.getFields());
    String group = visitExpressionList(node.getGroupExprList());
    String options =
        UnresolvedPlanHelper.isCalciteEnabled(settings)
            ? StringUtils.format(
                "countield='%s' showcount=%s usenull=%s ", countField, showCount, useNull)
            : "";
    return StringUtils.format(
        "%s | %s %d %s%s",
        child,
        node.getCommandType().name().toLowerCase(),
        noOfResults,
        options,
        String.join(" ", fields, groupBy(group)).trim());
  }

  /** Build {@link LogicalProject} or {@link LogicalRemove} from {@link Field}. */
  @Override
  public String visitProject(Project node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String arg = "+";
    String fields = visitExpressionList(node.getProjectList());

    if (Strings.isNullOrEmpty(fields)) {
      return child;
    }

    if (node.hasArgument()) {
      Argument argument = node.getArgExprList().get(0);
      Boolean exclude = (Boolean) argument.getValue().getValue();
      if (exclude) {
        arg = "-";
      }
    }
    return StringUtils.format("%s | fields %s %s", child, arg, fields);
  }

  /** Build {@link LogicalEval}. */
  @Override
  public String visitEval(Eval node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    ImmutableList.Builder<Pair<String, String>> expressionsBuilder = new ImmutableList.Builder<>();
    for (Let let : node.getExpressionList()) {
      String expression = visitExpression(let.getExpression());
      String target = let.getVar().getField().toString();
      expressionsBuilder.add(ImmutablePair.of(target, expression));
    }
    String expressions =
        expressionsBuilder.build().stream()
            .map(pair -> StringUtils.format("%s" + "=%s", MASK_COLUMN, pair.getRight()))
            .collect(Collectors.joining(" "));
    return StringUtils.format("%s | eval %s", child, expressions);
  }

  @Override
  public String visitExpand(Expand node, String context) {
    String child = node.getChild().getFirst().accept(this, context);
    String field = visitExpression(node.getField());

    return StringUtils.format("%s | expand %s", child, field);
  }

  @Override
  public String visitMvCombine(MvCombine node, String context) {
    String child = node.getChild().getFirst().accept(this, context);
    String field = visitExpression(node.getField());

    return StringUtils.format("%s | mvcombine delim=%s %s", child, MASK_LITERAL, field);
  }

  /** Build {@link LogicalSort}. */
  @Override
  public String visitSort(Sort node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    // the first options is {"count": "integer"}
    Integer count = node.getCount();
    String sortList = visitFieldList(node.getSortList());
    if (count != 0) {
      return StringUtils.format("%s | sort %d %s", child, count, sortList);
    } else {
      return StringUtils.format("%s | sort %s", child, sortList);
    }
  }

  /** Build {@link LogicalDedupe}. */
  @Override
  public String visitDedupe(Dedupe node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String fields = visitFieldList(node.getFields());
    List<Argument> options = node.getOptions();
    Integer allowedDuplication = (Integer) options.get(0).getValue().getValue();
    Boolean keepEmpty = (Boolean) options.get(1).getValue().getValue();
    Boolean consecutive = (Boolean) options.get(2).getValue().getValue();

    return StringUtils.format(
        "%s | dedup %s %d keepempty=%b consecutive=%b",
        child, fields, allowedDuplication, keepEmpty, consecutive);
  }

  @Override
  public String visitHead(Head node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    Integer size = node.getSize();
    return StringUtils.format("%s | head %d", child, size);
  }

  @Override
  public String visitReverse(Reverse node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    return StringUtils.format("%s | reverse", child);
  }

  @Override
  public String visitChart(Chart node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    StringBuilder chartCommand = new StringBuilder();

    // Check if this is a timechart by looking for timestamp span in rowSplit
    boolean isTimechart = isTimechartNode(node);
    chartCommand.append(isTimechart ? " | timechart" : " | chart");

    for (Argument arg : node.getArguments()) {
      String argName = arg.getArgName();
      // Skip the auto-generated "top" parameter that's added when limit is specified
      if ("top".equals(argName)) {
        continue;
      }

      switch (argName) {
        case "limit", "useother", "usenull", "otherstr", "nullstr" ->
            chartCommand.append(" ").append(argName).append("=").append(MASK_LITERAL);
        case "spanliteral" -> chartCommand.append(" span=").append(MASK_LITERAL);
        case "timefield" ->
            chartCommand.append(" ").append(argName).append("=").append(MASK_TIMESTAMP_COLUMN);
        default ->
            throw new NotImplementedException(
                StringUtils.format("Please implement anonymizer for arg: %s", argName));
      }
    }

    chartCommand.append(" ").append(visitExpression(node.getAggregationFunction()));

    if (node.getRowSplit() != null && node.getColumnSplit() != null) {
      chartCommand.append(" by");
      // timechart command does not have to explicit the by-timestamp field clause
      if (!isTimechart) chartCommand.append(" ").append(visitExpression(node.getRowSplit()));
      chartCommand.append(" ").append(visitExpression(node.getColumnSplit()));
    } else if (node.getRowSplit() != null && !isTimechart) {
      chartCommand.append(" by ").append(visitExpression(node.getRowSplit()));
    } else if (node.getColumnSplit() != null) {
      chartCommand.append(" by ").append(visitExpression(node.getColumnSplit()));
    }

    return StringUtils.format("%s%s", child, chartCommand.toString());
  }

  private boolean isTimechartNode(Chart node) {
    // A Chart node represents a timechart if it has a rowSplit that's an alias containing
    // a span on the implicit timestamp field
    if (node.getRowSplit() instanceof Alias) {
      Alias alias = (Alias) node.getRowSplit();
      if (alias.getDelegated() instanceof Span) {
        Span span = (Span) alias.getDelegated();
        String timeFieldName =
            Optional.ofNullable(ArgumentMap.of(node.getArguments()).get("timefield"))
                .map(Literal::toString)
                .orElse(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP);
        return span.getField() instanceof Field
            && timeFieldName.equals(((Field) span.getField()).getField().toString());
      }
    }
    return false;
  }

  public String visitRex(Rex node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String field = visitExpression(node.getField());
    String pattern = "\"" + MASK_LITERAL + "\"";
    StringBuilder command = new StringBuilder();

    command.append(
        String.format(
            "%s | rex field=%s mode=%s %s",
            child, field, node.getMode().toString().toLowerCase(), pattern));

    if (node.getMaxMatch().isPresent()) {
      command.append(" max_match=").append(MASK_LITERAL);
    }

    if (node.getOffsetField().isPresent()) {
      command.append(" offset_field=").append(MASK_COLUMN);
    }

    return command.toString();
  }

  @Override
  public String visitParse(Parse node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String source = visitExpression(node.getSourceField());
    String regex = node.getPattern().toString();
    String commandName;

    switch (node.getParseMethod()) {
      case ParseMethod.PATTERNS:
        commandName = "patterns";
        break;
      case ParseMethod.GROK:
        commandName = "grok";
        break;
      default:
        commandName = "parse";
        break;
    }
    return ParseMethod.PATTERNS.equals(node.getParseMethod()) && regex.isEmpty()
        ? StringUtils.format("%s | %s %s", child, commandName, source)
        : StringUtils.format("%s | %s %s '%s'", child, commandName, source, MASK_LITERAL);
  }

  @Override
  public String visitRegex(Regex node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String operator = node.isNegated() ? Regex.NOT_EQUALS_OPERATOR : Regex.EQUALS_OPERATOR;
    String pattern = MASK_LITERAL;

    String field = visitExpression(node.getField());
    return StringUtils.format("%s | regex %s%s%s", child, field, operator, pattern);
  }

  @Override
  public String visitFlatten(Flatten node, String context) {
    String child = node.getChild().getFirst().accept(this, context);
    String field = visitExpression(node.getField());
    return StringUtils.format("%s | flatten %s", child, field);
  }

  @Override
  public String visitTrendline(Trendline node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String computations = visitExpressionList(node.getComputations(), " ");
    return StringUtils.format("%s | trendline %s", child, computations);
  }

  @Override
  public String visitTranspose(Transpose node, String context) {
    if (node.getChild().isEmpty()) {
      return "source=*** | transpose";
    }
    String child = node.getChild().get(0).accept(this, context);
    StringBuilder anonymized = new StringBuilder(StringUtils.format("%s | transpose", child));
    java.util.Map<String, Argument> arguments = node.getArguments();

    if (arguments.containsKey("number")) {
      Argument numberArg = arguments.get("number");
      if (numberArg != null) {
        anonymized.append(StringUtils.format(" %s", numberArg.getValue()));
      }
    }
    if (arguments.containsKey("columnName")) {
      anonymized.append(StringUtils.format(" %s=***", "column_name"));
    }
    return anonymized.toString();
  }

  @Override
  public String visitAppendCol(AppendCol node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    UnresolvedPlan relation = getRelation(node);
    transformPlanToAttachChild(node.getSubSearch(), relation);
    String subsearch = anonymizeData(node.getSubSearch());
    String subsearchWithoutRelation = subsearch.substring(subsearch.indexOf("|") + 1);
    return StringUtils.format(
        "%s | appendcol override=%s [%s ]", child, node.isOverride(), subsearchWithoutRelation);
  }

  @Override
  public String visitAppend(Append node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String subsearch = anonymizeData(node.getSubSearch());
    return StringUtils.format("%s | append [%s ]", child, subsearch);
  }

  @Override
  public String visitMultisearch(Multisearch node, String context) {
    List<String> anonymizedSubsearches = new ArrayList<>();

    for (UnresolvedPlan subsearch : node.getSubsearches()) {
      String anonymizedSubsearch = anonymizeData(subsearch);
      anonymizedSubsearch = "search " + anonymizedSubsearch;
      anonymizedSubsearch =
          anonymizedSubsearch
              .replaceAll("\\bsource=\\w+", "source=table") // Replace table names after source=
              .replaceAll(
                  "\\b(?!source|fields|where|stats|head|tail|sort|eval|rename|multisearch|search|table|identifier|\\*\\*\\*)\\w+(?=\\s*[<>=!])",
                  "identifier") // Replace field names before operators
              .replaceAll(
                  "\\b(?!source|fields|where|stats|head|tail|sort|eval|rename|multisearch|search|table|identifier|\\*\\*\\*)\\w+(?=\\s*,)",
                  "identifier") // Replace field names before commas
              .replaceAll(
                  "fields"
                      + " \\+\\s*\\b(?!source|fields|where|stats|head|tail|sort|eval|rename|multisearch|search|table|identifier|\\*\\*\\*)\\w+",
                  "fields + identifier") // Replace field names after 'fields +'
              .replaceAll(
                  "fields"
                      + " \\+\\s*identifier,\\s*\\b(?!source|fields|where|stats|head|tail|sort|eval|rename|multisearch|search|table|identifier|\\*\\*\\*)\\w+",
                  "fields + identifier,identifier"); // Handle multiple fields
      anonymizedSubsearches.add(StringUtils.format("[%s]", anonymizedSubsearch));
    }

    return StringUtils.format("| multisearch %s", String.join(" ", anonymizedSubsearches));
  }

  @Override
  public String visitValues(Values node, String context) {
    // In case legacy SQL relies on it, return empty to fail open anyway.
    // Don't expect it to fail the query execution.
    return "";
  }

  private String visitFieldList(List<Field> fieldList) {
    return fieldList.stream().map(this::visitExpression).collect(Collectors.joining(","));
  }

  private String visitExpressionList(List<? extends UnresolvedExpression> expressionList) {
    return visitExpressionList(expressionList, ",");
  }

  private String visitExpressionList(
      List<? extends UnresolvedExpression> expressionList, String delimiter) {
    return expressionList.isEmpty()
        ? ""
        : expressionList.stream().map(this::visitExpression).collect(Collectors.joining(delimiter));
  }

  private String visitExpression(UnresolvedExpression expression) {
    return expressionAnalyzer.analyze(expression, null);
  }

  @Override
  public String visitAppendPipe(AppendPipe node, String context) {
    Values emptyValue = new Values(null);
    UnresolvedPlan childNode = node.getSubQuery();
    while (childNode != null && !childNode.getChild().isEmpty()) {
      childNode = (UnresolvedPlan) childNode.getChild().get(0);
    }
    childNode.attach(emptyValue);
    String child = node.getChild().get(0).accept(this, context);
    String subPipeline = anonymizeData(node.getSubQuery());
    return StringUtils.format("%s | appendpipe [%s]", child, subPipeline);
  }

  @Override
  public String visitFillNull(FillNull node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    List<Pair<Field, UnresolvedExpression>> fieldFills = node.getReplacementPairs();

    // Check if using value= syntax (added in 3.4)
    if (node.isUseValueSyntax()) {
      if (fieldFills.isEmpty()) {
        return StringUtils.format("%s | fillnull value=%s", child, MASK_LITERAL);
      }
      return StringUtils.format(
          "%s | fillnull value=%s %s",
          child,
          MASK_LITERAL,
          fieldFills.stream()
              .map(n -> visitExpression(n.getLeft()))
              .collect(Collectors.joining(" ")));
    }

    // Distinguish between with...in and using based on whether all values are the same
    if (fieldFills.isEmpty()) {
      return StringUtils.format("%s | fillnull with %s", child, MASK_LITERAL);
    }
    final UnresolvedExpression firstReplacement = fieldFills.getFirst().getRight();
    if (fieldFills.stream().allMatch(n -> firstReplacement == n.getRight())) {
      // All fields use same replacement value -> with...in syntax
      return StringUtils.format(
          "%s | fillnull with %s in %s",
          child,
          MASK_LITERAL,
          fieldFills.stream()
              .map(n -> visitExpression(n.getLeft()))
              .collect(Collectors.joining(", ")));
    } else {
      // Different replacement values per field -> using syntax
      return StringUtils.format(
          "%s | fillnull using %s",
          child,
          fieldFills.stream()
              .map(n -> StringUtils.format("%s = %s", visitExpression(n.getLeft()), MASK_LITERAL))
              .collect(Collectors.joining(", ")));
    }
  }

  @Override
  public String visitSpath(SPath node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    StringBuilder builder = new StringBuilder();
    builder.append(child).append(" | spath");
    if (node.getInField() != null) {
      builder.append(" input=").append(MASK_COLUMN);
    }
    if (node.getOutField() != null) {
      builder.append(" output=").append(MASK_COLUMN);
    }
    if (node.getPath() != null) {
      builder.append(" path=").append(MASK_COLUMN);
    }
    return builder.toString();
  }

  public void appendAddTotalsOptionParameters(
      List<Field> fieldList, java.util.Map<String, Literal> options, StringBuilder builder) {

    if (!fieldList.isEmpty()) {
      builder.append(visitExpressionList(fieldList, " "));
    }
    if (!options.isEmpty()) {
      for (String key : options.keySet()) {
        String value = options.get(key).toString();
        if (value.matches(".*\\s.*")) {
          value = StringUtils.format("'%s'", value);
        }
        builder.append(" ").append(key).append("=").append(value);
      }
    }
  }

  @Override
  public String visitAddTotals(AddTotals node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    StringBuilder builder = new StringBuilder();
    builder.append(child).append(" | addtotals");
    appendAddTotalsOptionParameters(node.getFieldList(), node.getOptions(), builder);
    return builder.toString();
  }

  @Override
  public String visitAddColTotals(AddColTotals node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    StringBuilder builder = new StringBuilder();
    builder.append(child).append(" | addcoltotals");
    appendAddTotalsOptionParameters(node.getFieldList(), node.getOptions(), builder);
    return builder.toString();
  }

  @Override
  public String visitPatterns(Patterns node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String sourceField = visitExpression(node.getSourceField());
    StringBuilder builder = new StringBuilder();
    builder.append(child).append(" | patterns ").append(sourceField);
    if (!node.getPartitionByList().isEmpty()) {
      String partitionByList = visitExpressionList(node.getPartitionByList());
      builder.append(" by ").append(partitionByList);
    }
    builder.append(" method=").append(node.getPatternMethod().toString());
    builder.append(" mode=").append(node.getPatternMode().toString());
    builder.append(" max_sample_count=").append(visitExpression(node.getPatternMaxSampleCount()));
    builder.append(" buffer_limit=").append(visitExpression(node.getPatternBufferLimit()));
    builder.append(" new_field=").append(MASK_COLUMN);
    if (!node.getArguments().isEmpty()) {
      for (java.util.Map.Entry<String, Literal> entry : node.getArguments().entrySet()) {
        builder.append(
            String.format(
                Locale.ROOT, " %s=%s", entry.getKey(), visitExpression(entry.getValue())));
      }
    }

    return builder.toString();
  }

  private String groupBy(String groupBy) {
    return Strings.isNullOrEmpty(groupBy) ? "" : StringUtils.format("by %s", groupBy);
  }

  /** Expression Anonymizer. */
  private static class AnonymizerExpressionAnalyzer extends AbstractNodeVisitor<String, String> {
    private final PPLQueryDataAnonymizer queryAnonymizer;

    public AnonymizerExpressionAnalyzer(PPLQueryDataAnonymizer queryAnonymizer) {
      this.queryAnonymizer = queryAnonymizer;
    }

    public String analyze(UnresolvedExpression unresolved, String context) {
      return unresolved.accept(this, context);
    }

    @Override
    public String visitLiteral(Literal node, String context) {
      return MASK_LITERAL;
    }

    @Override
    public String visitInterval(Interval node, String context) {
      String value = node.getValue().accept(this, context);
      String unit = node.getUnit().name();
      return StringUtils.format("INTERVAL %s %s", value, unit);
    }

    @Override
    public String visitAnd(And node, String context) {
      String left = node.getLeft().accept(this, context);
      String right = node.getRight().accept(this, context);
      return StringUtils.format("%s and %s", left, right);
    }

    @Override
    public String visitOr(Or node, String context) {
      String left = node.getLeft().accept(this, context);
      String right = node.getRight().accept(this, context);
      return StringUtils.format("%s or %s", left, right);
    }

    @Override
    public String visitXor(Xor node, String context) {
      String left = node.getLeft().accept(this, context);
      String right = node.getRight().accept(this, context);
      return StringUtils.format("%s xor %s", left, right);
    }

    @Override
    public String visitNot(Not node, String context) {
      String expr = node.getExpression().accept(this, context);
      return StringUtils.format("not %s", expr);
    }

    @Override
    public String visitAggregateFunction(AggregateFunction node, String context) {
      String arg = node.getField().accept(this, context);
      return StringUtils.format("%s(%s)", node.getFuncName(), arg);
    }

    @Override
    public String visitSpan(Span node, String context) {
      String field = analyze(node.getField(), context);
      String value = analyze(node.getValue(), context);
      return StringUtils.format("span(%s, %s %s)", field, value, node.getUnit().getName());
    }

    @Override
    public String visitFunction(Function node, String context) {
      // For mvmap, unwrap the implicit lambda to show original format
      if ("mvmap".equalsIgnoreCase(node.getFuncName())
          && node.getFuncArgs().size() == 2
          && node.getFuncArgs().get(1) instanceof LambdaFunction) {
        String firstArg = analyze(node.getFuncArgs().get(0), context);
        LambdaFunction lambda = (LambdaFunction) node.getFuncArgs().get(1);
        String lambdaBody = analyze(lambda.getFunction(), context);
        return StringUtils.format("%s(%s,%s)", node.getFuncName(), firstArg, lambdaBody);
      }

      String arguments =
          node.getFuncArgs().stream()
              .map(unresolvedExpression -> analyze(unresolvedExpression, context))
              .collect(Collectors.joining(","));
      return StringUtils.format("%s(%s)", node.getFuncName(), arguments);
    }

    @Override
    public String visitLambdaFunction(LambdaFunction node, String context) {
      String args =
          node.getFuncArgs().stream()
              .map(arg -> maskField(arg.toString()))
              .collect(Collectors.joining(","));
      String function = analyze(node.getFunction(), context);
      return StringUtils.format("%s -> %s", args, function);
    }

    @Override
    public String visitWindowFunction(WindowFunction node, String context) {
      String function = analyze(node.getFunction(), context);
      String partitions =
          node.getPartitionByList().stream()
              .map(p -> analyze(p, context))
              .collect(Collectors.joining(","));
      if (partitions.isEmpty()) {
        return StringUtils.format("%s", function);
      } else {
        return StringUtils.format("%s by %s", function, partitions);
      }
    }

    @Override
    public String visitCompare(Compare node, String context) {
      String left = analyze(node.getLeft(), context);
      String right = analyze(node.getRight(), context);
      return StringUtils.format("%s %s %s", left, node.getOperator(), right);
    }

    @Override
    public String visitBetween(Between node, String context) {
      String value = analyze(node.getValue(), context);
      String left = analyze(node.getLowerBound(), context);
      String right = analyze(node.getUpperBound(), context);
      return StringUtils.format("%s between %s and %s", value, left, right);
    }

    @Override
    public String visitIn(In node, String context) {
      String field = analyze(node.getField(), context);
      return StringUtils.format("%s in (%s)", field, MASK_LITERAL);
    }

    @Override
    public String visitField(Field node, String context) {
      String fieldName = node.getField().toString();
      return maskField(fieldName);
    }

    @Override
    public String visitAllFields(AllFields node, String context) {
      return "";
    }

    @Override
    public String visitAllFieldsExcludeMeta(AllFieldsExcludeMeta node, String context) {
      return "";
    }

    @Override
    public String visitAlias(Alias node, String context) {
      String expr = node.getDelegated().accept(this, context);
      return StringUtils.format("%s", expr);
    }

    @Override
    public String visitTrendlineComputation(Trendline.TrendlineComputation node, String context) {
      final String dataField = node.getDataField().accept(this, context);
      final String aliasClause = " as " + MASK_COLUMN;
      final String computationType = node.getComputationType().name().toLowerCase(Locale.ROOT);
      return StringUtils.format(
          "%s(%d, %s)%s", computationType, node.getNumberOfDataPoints(), dataField, aliasClause);
    }

    @Override
    public String visitInSubquery(InSubquery node, String context) {
      String nodes =
          node.getChild().stream().map(c -> analyze(c, context)).collect(Collectors.joining(","));
      String subquery = queryAnonymizer.anonymizeData(node.getQuery());
      return StringUtils.format("(%s) in [ %s ]", nodes, subquery);
    }

    @Override
    public String visitScalarSubquery(ScalarSubquery node, String context) {
      String subquery = queryAnonymizer.anonymizeData(node.getQuery());
      return StringUtils.format("[ %s ]", subquery);
    }

    @Override
    public String visitExistsSubquery(ExistsSubquery node, String context) {
      String subquery = queryAnonymizer.anonymizeData(node.getQuery());
      return StringUtils.format("exists [ %s ]", subquery);
    }

    @Override
    public String visitCase(Case node, String context) {
      StringBuilder builder = new StringBuilder();
      builder.append("case(");
      for (When when : node.getWhenClauses()) {
        builder.append(analyze(when.getCondition(), context));
        builder.append(",");
        builder.append(analyze(when.getResult(), context));
        builder.append(",");
      }
      builder.deleteCharAt(builder.lastIndexOf(","));
      node.getElseClause()
          .ifPresent(
              elseClause -> {
                builder.append(" else ");
                builder.append(analyze(elseClause, context));
              });
      builder.append(")");
      return builder.toString();
    }

    @Override
    public String visitCast(Cast node, String context) {
      String expr = analyze(node.getExpression(), context);
      return StringUtils.format("cast(%s as %s)", expr, node.getConvertedType().toString());
    }

    @Override
    public String visitQualifiedName(
        org.opensearch.sql.ast.expression.QualifiedName node, String context) {
      return MASK_COLUMN;
    }
  }
}
