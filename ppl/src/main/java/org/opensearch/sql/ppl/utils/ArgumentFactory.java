/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.BooleanLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.DedupCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.FieldsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.IntegerLiteralContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.RareCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SortFieldContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.StatsCommandContext;
import static org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.TopCommandContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.antlr.v4.runtime.ParserRuleContext;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.common.utils.StringUtils;

/**
 * Utility class for extracting arguments from PPL command contexts.
 *
 * <p>This factory class provides static methods to parse ANTLR parser contexts and extract
 * command-specific arguments as Argument objects with proper default values. Each PPL command has
 * its own argument extraction method.
 */
public class ArgumentFactory {

  /**
   * Extracts arguments from fields command context.
   *
   * <p>Determines whether the fields command is in exclude mode based on the presence of MINUS
   * token in the command body.
   *
   * @param ctx FieldsCommandContext instance from ANTLR parser
   * @return List containing single exclude argument (true if MINUS present, false otherwise)
   */
  public static List<Argument> getArgumentList(FieldsCommandContext ctx) {
    return Collections.singletonList(
        ctx.fieldsCommandBody().MINUS() != null
            ? new Argument("exclude", new Literal(true, DataType.BOOLEAN))
            : new Argument("exclude", new Literal(false, DataType.BOOLEAN)));
  }

  /**
   * Extracts arguments from stats command context with default values.
   *
   * <p>Parses optional parameters: partitions, allnum, delim, and dedupsplit. Provides sensible
   * defaults when parameters are not specified.
   *
   * @param ctx StatsCommandContext instance from ANTLR parser
   * @return List of arguments with defaults: partitions=1, allnum=false, delim=" ",
   *     dedupsplit=false
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
   * Extracts arguments from dedup command context with default values.
   *
   * <p>Parses deduplication parameters: number of duplicates allowed, whether to keep empty values,
   * and consecutive deduplication mode.
   *
   * @param ctx DedupCommandContext instance from ANTLR parser
   * @return List of arguments with defaults: number=1, keepempty=false, consecutive=false
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
   * Extracts arguments from sort field context.
   *
   * <p>Determines sort order (ascending/descending) and field type for sorting. MINUS token
   * indicates descending order, type is determined from field expression.
   *
   * @param ctx SortFieldContext instance from ANTLR parser
   * @return List of arguments: asc (boolean), type (auto/ip/num/str/null)
   */
  public static List<Argument> getArgumentList(SortFieldContext ctx) {
    // Determine sort order: MINUS token indicates descending
    Argument ascArgument = new Argument("asc", new Literal(ctx.MINUS() == null, DataType.BOOLEAN));

    // Determine sort type from field expression
    Argument typeArgument = getSortTypeArgument(ctx);

    return Arrays.asList(ascArgument, typeArgument);
  }

  /**
   * Extracts sort type argument from sort field expression.
   *
   * @param ctx SortFieldContext containing the field expression
   * @return Argument with type value (auto/ip/num/str/null)
   */
  private static Argument getSortTypeArgument(SortFieldContext ctx) {
    if (ctx.sortFieldExpression().AUTO() != null) {
      return new Argument("type", new Literal("auto", DataType.STRING));
    } else if (ctx.sortFieldExpression().IP() != null) {
      return new Argument("type", new Literal("ip", DataType.STRING));
    } else if (ctx.sortFieldExpression().NUM() != null) {
      return new Argument("type", new Literal("num", DataType.STRING));
    } else if (ctx.sortFieldExpression().STR() != null) {
      return new Argument("type", new Literal("str", DataType.STRING));
    } else {
      return new Argument("type", new Literal(null, DataType.NULL));
    }
  }

  /**
   * Extracts arguments from top command context with default values.
   *
   * <p>Parses parameters for top N results: number of results, count field name, and whether to
   * show count in output.
   *
   * @param ctx TopCommandContext instance from ANTLR parser
   * @return List of arguments with defaults: noOfResults=10, countField="count", showCount=true
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
   * Extracts arguments from rare command context with default values.
   *
   * <p>Parses parameters for rare N results: number of results, count field name, and whether to
   * show count in output. Uses same defaults as top command.
   *
   * @param ctx RareCommandContext instance from ANTLR parser
   * @return List of arguments with defaults: noOfResults=10, countField="count", showCount=true
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
   * Parses parser context into appropriate Literal value.
   *
   * <p>Determines the literal type based on parser context type and converts the text value
   * accordingly. Supports integer, boolean, and string literals.
   *
   * @param ctx ParserRuleContext instance (IntegerLiteral, BooleanLiteral, or other)
   * @return Literal object with appropriate DataType and parsed value
   */
  private static Literal getArgumentValue(ParserRuleContext ctx) {
    return ctx instanceof IntegerLiteralContext
        ? new Literal(Integer.parseInt(ctx.getText()), DataType.INTEGER)
        : ctx instanceof BooleanLiteralContext
            ? new Literal(Boolean.valueOf(ctx.getText()), DataType.BOOLEAN)
            : new Literal(StringUtils.unquoteText(ctx.getText()), DataType.STRING);
  }
}
