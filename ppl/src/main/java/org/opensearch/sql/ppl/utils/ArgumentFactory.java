/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.antlr.v4.runtime.ParserRuleContext;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntegerLiteralContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RareCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortFieldContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TopCommandContext;

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
  public static List<Argument> getArgumentList(StatsCommandContext ctx) {
    return Arrays.asList(
        ctx.partitions != null
            ? new Argument("partitions", getArgumentValue(ctx.partitions))
            : new Argument("partitions", new Literal(1, DataType.INTEGER)),
        ctx.allnum != null
            ? new Argument("allnum", getArgumentValue(ctx.allnum))
            : new Argument("allnum", new Literal(false, DataType.BOOLEAN)),
        ctx.delim != null
            ? new Argument("delim", getArgumentValue(ctx.delim))
            : new Argument("delim", new Literal(" ", DataType.STRING)),
        ctx.dedupsplit != null
            ? new Argument("dedupsplit", getArgumentValue(ctx.dedupsplit))
            : new Argument("dedupsplit", new Literal(false, DataType.BOOLEAN)));
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
    return Arrays.asList(
        ctx.MINUS() != null
            ? new Argument("asc", new Literal(false, DataType.BOOLEAN))
            : new Argument("asc", new Literal(true, DataType.BOOLEAN)),
        ctx.sortFieldExpression().AUTO() != null
            ? new Argument("type", new Literal("auto", DataType.STRING))
            : ctx.sortFieldExpression().IP() != null
                ? new Argument("type", new Literal("ip", DataType.STRING))
                : ctx.sortFieldExpression().NUM() != null
                    ? new Argument("type", new Literal("num", DataType.STRING))
                    : ctx.sortFieldExpression().STR() != null
                        ? new Argument("type", new Literal("str", DataType.STRING))
                        : new Argument("type", new Literal(null, DataType.NULL)));
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx TopCommandContext instance
   * @return the list of arguments fetched from the top command
   */
  public static List<Argument> getArgumentList(TopCommandContext ctx) {
    return Arrays.asList(
        ctx.number != null
            ? new Argument("noOfResults", getArgumentValue(ctx.number))
            : new Argument("noOfResults", new Literal(10, DataType.INTEGER)),
        ctx.countfield != null
            ? new Argument("countField", getArgumentValue(ctx.countfield))
            : new Argument("countField", new Literal("count", DataType.STRING)),
        ctx.showcount != null
            ? new Argument("showCount", getArgumentValue(ctx.showcount))
            : new Argument("showCount", new Literal(true, DataType.BOOLEAN)));
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx RareCommandContext instance
   * @return the list of argument with default number of results for the rare command
   */
  public static List<Argument> getArgumentList(RareCommandContext ctx) {
    return Arrays.asList(
        ctx.number != null
            ? new Argument("noOfResults", getArgumentValue(ctx.number))
            : new Argument("noOfResults", new Literal(10, DataType.INTEGER)),
        ctx.countfield != null
            ? new Argument("countField", getArgumentValue(ctx.countfield))
            : new Argument("countField", new Literal("count", DataType.STRING)),
        ctx.showcount != null
            ? new Argument("showCount", getArgumentValue(ctx.showcount))
            : new Argument("showCount", new Literal(true, DataType.BOOLEAN)));
  }

  /**
   * parse argument value into Literal.
   *
   * @param ctx ParserRuleContext instance
   * @return Literal
   */
  private static Literal getArgumentValue(ParserRuleContext ctx) {
    return ctx instanceof IntegerLiteralContext
        ? new Literal(Integer.parseInt(ctx.getText()), DataType.INTEGER)
        : ctx instanceof BooleanLiteralContext
            ? new Literal(Boolean.valueOf(ctx.getText()), DataType.BOOLEAN)
            : new Literal(StringUtils.unquoteText(ctx.getText()), DataType.STRING);
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
    try {
      if (ctx == null) return Join.JoinType.INNER;
      if (ctx.INNER() != null) return Join.JoinType.valueOf(ctx.INNER().getText());
      if (ctx.SEMI() != null) return Join.JoinType.valueOf(ctx.SEMI().getText());
      if (ctx.ANTI() != null) return Join.JoinType.valueOf(ctx.ANTI().getText());
      if (ctx.LEFT() != null) return Join.JoinType.valueOf(ctx.LEFT().getText());
      if (ctx.RIGHT() != null) return Join.JoinType.valueOf(ctx.RIGHT().getText());
      if (ctx.CROSS() != null) return Join.JoinType.valueOf(ctx.CROSS().getText());
      if (ctx.FULL() != null) return Join.JoinType.valueOf(ctx.FULL().getText());
      if (ctx.OUTER() != null) return Join.JoinType.LEFT; // Special case
      return Join.JoinType.INNER; // Default case
    } catch (Exception e) {
      throw new SemanticCheckException(String.format("Unsupported join type %s", ctx.getText()));
    }
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
    if (ctx.OUTER() != null) return Join.JoinType.LEFT; // Special case
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
