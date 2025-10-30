/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.antlr.v4.runtime.ParserRuleContext;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DecimalLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DefaultSortFieldContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntegerLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.PrefixSortFieldContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortFieldContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SuffixSortFieldContext;

/** Util class to get all arguments as a list from the PPL command. */
public class ArgumentFactory {

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx FieldsCommandContext instance
   * @return the list of arguments fetched from the fields command
   */
  public static List<Argument> getArgumentList(FieldsCommandContext ctx) {
    return Collections.singletonList(
        ctx.fieldsCommandBody().MINUS() != null
            ? new Argument("exclude", new Literal(true, DataType.BOOLEAN))
            : new Argument("exclude", new Literal(false, DataType.BOOLEAN)));
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx StatsCommandContext instance
   * @return the list of arguments fetched from the stats command
   */
  public static List<Argument> getArgumentList(
      OpenSearchPPLParser.StatsCommandContext ctx, Settings settings) {
    OpenSearchPPLParser.StatsArgsContext ctx1 = ctx.statsArgs();
    OpenSearchPPLParser.DedupSplitArgContext ctx2 = ctx.dedupSplitArg();
    List<Argument> list =
        new ArrayList<>(
            Arrays.asList(
                ctx1.partitionsArg() != null && !ctx1.partitionsArg().isEmpty()
                    ? new Argument("partitions", getArgumentValue(ctx1.partitionsArg(0).partitions))
                    : new Argument("partitions", Literal.ONE),
                ctx1.allnumArg() != null && !ctx1.allnumArg().isEmpty()
                    ? new Argument("allnum", getArgumentValue(ctx1.allnumArg(0).allnum))
                    : new Argument("allnum", Literal.FALSE),
                ctx1.delimArg() != null && !ctx1.delimArg().isEmpty()
                    ? new Argument("delim", getArgumentValue(ctx1.delimArg(0).delim))
                    : new Argument("delim", new Literal(" ", DataType.STRING)),
                ctx1.bucketNullableArg() != null && !ctx1.bucketNullableArg().isEmpty()
                    ? new Argument(
                        Argument.BUCKET_NULLABLE,
                        getArgumentValue(ctx1.bucketNullableArg(0).bucket_nullable))
                    : new Argument(
                        Argument.BUCKET_NULLABLE,
                        legacyPreferred(settings) ? Literal.TRUE : Literal.FALSE)));
    if (ctx2 != null) {
      list.add(new Argument("dedupsplit", getArgumentValue(ctx2.dedupsplit)));
    } else {
      list.add(new Argument("dedupsplit", Literal.FALSE));
    }
    return list;
  }

  private static boolean legacyPreferred(Settings settings) {
    return settings == null
        || settings.getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED) == null
        || Boolean.TRUE.equals(settings.getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED));
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx DedupCommandContext instance
   * @return the list of arguments fetched from the dedup command
   */
  public static List<Argument> getArgumentList(DedupCommandContext ctx) {
    return Arrays.asList(
        ctx.number != null
            ? new Argument("number", getArgumentValue(ctx.number))
            : new Argument("number", new Literal(1, DataType.INTEGER)),
        ctx.keepempty != null
            ? new Argument("keepempty", getArgumentValue(ctx.keepempty))
            : new Argument("keepempty", new Literal(false, DataType.BOOLEAN)),
        ctx.consecutive != null
            ? new Argument("consecutive", getArgumentValue(ctx.consecutive))
            : new Argument("consecutive", new Literal(false, DataType.BOOLEAN)));
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx SortFieldContext instance
   * @return the list of arguments fetched from the sort field in sort command
   */
  public static List<Argument> getArgumentList(SortFieldContext ctx) {
    if (ctx instanceof PrefixSortFieldContext) {
      return getArgumentList((PrefixSortFieldContext) ctx);
    } else if (ctx instanceof SuffixSortFieldContext) {
      return getArgumentList((SuffixSortFieldContext) ctx);
    } else {
      return getArgumentList((DefaultSortFieldContext) ctx);
    }
  }

  /**
   * Get list of {@link Argument} for prefix sort field (+/- syntax).
   *
   * @param ctx PrefixSortFieldContext instance
   * @return the list of arguments fetched from the prefix sort field
   */
  public static List<Argument> getArgumentList(PrefixSortFieldContext ctx) {
    return Arrays.asList(
        ctx.MINUS() != null
            ? new Argument("asc", new Literal(false, DataType.BOOLEAN))
            : new Argument("asc", new Literal(true, DataType.BOOLEAN)),
        getTypeArgument(ctx.sortFieldExpression()));
  }

  /**
   * Get list of {@link Argument} for suffix sort field (asc/desc syntax).
   *
   * @param ctx SuffixSortFieldContext instance
   * @return the list of arguments fetched from the suffix sort field
   */
  public static List<Argument> getArgumentList(SuffixSortFieldContext ctx) {
    return Arrays.asList(
        (ctx.DESC() != null || ctx.D() != null)
            ? new Argument("asc", new Literal(false, DataType.BOOLEAN))
            : new Argument("asc", new Literal(true, DataType.BOOLEAN)),
        getTypeArgument(ctx.sortFieldExpression()));
  }

  /**
   * Get list of {@link Argument} for default sort field (no direction specified).
   *
   * @param ctx DefaultSortFieldContext instance
   * @return the list of arguments fetched from the default sort field
   */
  public static List<Argument> getArgumentList(DefaultSortFieldContext ctx) {
    return Arrays.asList(
        new Argument("asc", new Literal(true, DataType.BOOLEAN)),
        getTypeArgument(ctx.sortFieldExpression()));
  }

  /** Helper method to get type argument from sortFieldExpression. */
  private static Argument getTypeArgument(OpenSearchPPLParser.SortFieldExpressionContext ctx) {
    if (ctx.AUTO() != null) {
      return new Argument("type", new Literal("auto", DataType.STRING));
    } else if (ctx.IP() != null) {
      return new Argument("type", new Literal("ip", DataType.STRING));
    } else if (ctx.NUM() != null) {
      return new Argument("type", new Literal("num", DataType.STRING));
    } else if (ctx.STR() != null) {
      return new Argument("type", new Literal("str", DataType.STRING));
    } else {
      return new Argument("type", new Literal(null, DataType.NULL));
    }
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx RareCommandContext instance
   * @param settings Settings instance
   * @return the list of argument with default number of results for the rare command
   */
  public static List<Argument> getArgumentList(
      OpenSearchPPLParser.RareTopCommandContext ctx, Settings settings) {
    List<Argument> list = new ArrayList<>();
    Optional<OpenSearchPPLParser.RareTopOptionContext> opt =
        ctx.rareTopOption().stream().filter(op -> op.countField != null).findFirst();
    list.add(
        new Argument(
            RareTopN.Option.countField.name(),
            opt.isPresent()
                ? getArgumentValue(opt.get().countField)
                : new Literal("count", DataType.STRING)));
    opt = ctx.rareTopOption().stream().filter(op -> op.showCount != null).findFirst();
    list.add(
        new Argument(
            RareTopN.Option.showCount.name(),
            opt.isPresent() ? getArgumentValue(opt.get().showCount) : Literal.TRUE));
    opt = ctx.rareTopOption().stream().filter(op -> op.useNull != null).findFirst();
    list.add(
        new Argument(
            RareTopN.Option.useNull.name(),
            opt.isPresent()
                ? getArgumentValue(opt.get().useNull)
                : legacyPreferred(settings) ? Literal.TRUE : Literal.FALSE));
    return list;
  }

  /**
   * parse argument value into Literal.
   *
   * @param ctx ParserRuleContext instance
   * @return Literal
   */
  private static Literal getArgumentValue(ParserRuleContext ctx) {
    if (ctx instanceof IntegerLiteralContext) {
      return new Literal(Integer.parseInt(ctx.getText()), DataType.INTEGER);
    } else if (ctx instanceof DecimalLiteralContext) {
      return new Literal(Double.parseDouble(ctx.getText()), DataType.DOUBLE);
    } else if (ctx instanceof BooleanLiteralContext) {
      return new Literal(Boolean.valueOf(ctx.getText()), DataType.BOOLEAN);
    } else {
      return new Literal(StringUtils.unquoteText(ctx.getText()), DataType.STRING);
    }
  }

  /**
   * parse argument value into Literal.
   *
   * @param ctx ParserRuleContext instance
   * @return Literal
   */
  public static Argument getArgumentValue(OpenSearchPPLParser.JoinTypeContext ctx) {
    Join.JoinType type = getJoinType(ctx);
    return new Argument("type", new Literal(type.name(), DataType.STRING));
  }

  public static Join.JoinType getJoinType(OpenSearchPPLParser.SqlLikeJoinTypeContext ctx) {
    if (ctx == null) return Join.JoinType.INNER;
    if (ctx.INNER() != null) return Join.JoinType.INNER;
    if (ctx.SEMI() != null) return Join.JoinType.SEMI;
    if (ctx.ANTI() != null) return Join.JoinType.ANTI;
    if (ctx.LEFT() != null) return Join.JoinType.LEFT;
    if (ctx.RIGHT() != null) return Join.JoinType.RIGHT;
    if (ctx.CROSS() != null) return Join.JoinType.CROSS;
    if (ctx.FULL() != null) return Join.JoinType.FULL;
    if (ctx.OUTER() != null) return Join.JoinType.LEFT;
    throw new SemanticCheckException(String.format("Unsupported join type %s", ctx.getText()));
  }

  public static Join.JoinType getJoinType(OpenSearchPPLParser.JoinTypeContext ctx) {
    if (ctx == null) return Join.JoinType.INNER;
    if (ctx.INNER() != null) return Join.JoinType.INNER;
    if (ctx.SEMI() != null) return Join.JoinType.SEMI;
    if (ctx.ANTI() != null) return Join.JoinType.ANTI;
    if (ctx.LEFT() != null) return Join.JoinType.LEFT;
    if (ctx.RIGHT() != null) return Join.JoinType.RIGHT;
    if (ctx.CROSS() != null) return Join.JoinType.CROSS;
    if (ctx.FULL() != null) return Join.JoinType.FULL;
    if (ctx.OUTER() != null) return Join.JoinType.LEFT;
    throw new SemanticCheckException(String.format("Unsupported join type %s", ctx.getText()));
  }

  public static Join.JoinType getJoinType(Argument.ArgumentMap argumentMap) {
    Join.JoinType joinType;
    String type = argumentMap.get("type").toString();
    if (type.equalsIgnoreCase(Join.JoinType.INNER.name())) {
      joinType = Join.JoinType.INNER;
    } else if (type.equalsIgnoreCase(Join.JoinType.SEMI.name())) {
      joinType = Join.JoinType.SEMI;
    } else if (type.equalsIgnoreCase(Join.JoinType.ANTI.name())) {
      joinType = Join.JoinType.ANTI;
    } else if (type.equalsIgnoreCase(Join.JoinType.LEFT.name())) {
      joinType = Join.JoinType.LEFT;
    } else if (type.equalsIgnoreCase(Join.JoinType.RIGHT.name())) {
      joinType = Join.JoinType.RIGHT;
    } else if (type.equalsIgnoreCase(Join.JoinType.CROSS.name())) {
      joinType = Join.JoinType.CROSS;
    } else if (type.equalsIgnoreCase(Join.JoinType.FULL.name())) {
      joinType = Join.JoinType.FULL;
    } else if (type.equalsIgnoreCase("OUTER")) {
      joinType = Join.JoinType.LEFT;
    } else {
      throw new SemanticCheckException(String.format("Supported join type %s", type));
    }
    return joinType;
  }
}
