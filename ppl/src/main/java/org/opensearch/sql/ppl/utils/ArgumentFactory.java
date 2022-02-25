/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.utils;

import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DecimalLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntegerLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RareCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortFieldContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TopCommandContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.KmeansCommandContext;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.AdCommandContext;


/**
 * Util class to get all arguments as a list from the PPL command.
 */
public class ArgumentFactory {

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx FieldsCommandContext instance
   * @return the list of arguments fetched from the fields command
   */
  public static List<Argument> getArgumentList(FieldsCommandContext ctx) {
    return Collections.singletonList(
        ctx.MINUS() != null
            ? new Argument("exclude", new Literal(true, DataType.BOOLEAN))
            : new Argument("exclude", new Literal(false, DataType.BOOLEAN))
    );
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
            : new Argument("dedupsplit", new Literal(false, DataType.BOOLEAN))
    );
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
            : new Argument("consecutive", new Literal(false, DataType.BOOLEAN))
    );
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
            : new Argument("type", new Literal(null, DataType.NULL))
    );
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx TopCommandContext instance
   * @return the list of arguments fetched from the top command
   */
  public static List<Argument> getArgumentList(TopCommandContext ctx) {
    return Collections.singletonList(
        ctx.number != null
            ? new Argument("noOfResults", getArgumentValue(ctx.number))
            : new Argument("noOfResults", new Literal(10, DataType.INTEGER))
    );
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx RareCommandContext instance
   * @return the list of argument with default number of results for the rare command
   */
  public static List<Argument> getArgumentList(RareCommandContext ctx) {
    return Collections
        .singletonList(new Argument("noOfResults", new Literal(10, DataType.INTEGER)));
  }

  /**
   * Get list of {@link Argument}.
   *
   * @param ctx KmeansCommandContext instance
   * @return the list of arguments fetched from the kmeans command
   */
  public static List<Argument> getArgumentList(KmeansCommandContext ctx) {
    // TODO: add iterations and distanceType parameters for Kemans
    return Collections
            .singletonList(new Argument("k", getArgumentValue(ctx.k)));
  }

  /**
   * Get map of {@link Argument}.
   *
   * @param ctx ADCommandContext instance
   * @return the list of arguments fetched from the AD command
   */
  public static Map<String, Literal> getArgumentMap(AdCommandContext ctx) {
    return new HashMap<String, Literal>() {{
      put("shingle_size", (ctx.shingle_size != null)
              ? getArgumentValue(ctx.shingle_size)
              : new Literal(8, DataType.INTEGER));
      put("time_decay", (ctx.time_decay != null)
              ? getArgumentValue(ctx.time_decay)
              : new Literal(0.0001, DataType.DOUBLE));
      put("time_field", (ctx.time_field != null)
              ? getArgumentValue(ctx.time_field)
              : new Literal(null, DataType.STRING));
    }};
  }

  private static Literal getArgumentValue(ParserRuleContext ctx) {
    return ctx instanceof IntegerLiteralContext
        ? new Literal(Integer.parseInt(ctx.getText()), DataType.INTEGER)
        : ctx instanceof BooleanLiteralContext
        ? new Literal(Boolean.valueOf(ctx.getText()), DataType.BOOLEAN)
        : ctx instanceof DecimalLiteralContext
        ? new Literal(Double.valueOf(ctx.getText()), DataType.DOUBLE)
        : new Literal(StringUtils.unquoteText(ctx.getText()), DataType.STRING);
  }

}
