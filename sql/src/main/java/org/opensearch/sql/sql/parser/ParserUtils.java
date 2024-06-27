/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ast.tree.Sort.NullOrder;
import static org.opensearch.sql.ast.tree.Sort.SortOption;
import static org.opensearch.sql.ast.tree.Sort.SortOrder;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OrderByElementContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.sql.ast.Node;

/** Parser Utils Class. */
@UtilityClass
public class ParserUtils {

  /** Get original text in query. */
  public static String getTextInQuery(ParserRuleContext ctx, String queryString) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop();
    return queryString.substring(start.getStartIndex(), stop.getStopIndex() + 1);
  }

  /** Create sort option from syntax tree node. */
  public static SortOption createSortOption(OrderByElementContext orderBy) {
    return new SortOption(
        createSortOrder(orderBy.order), createNullOrder(orderBy.FIRST(), orderBy.LAST()));
  }

  /** Create sort order for sort option use from ASC/DESC token. */
  public static SortOrder createSortOrder(Token ctx) {
    if (ctx == null) {
      return null;
    }
    return SortOrder.valueOf(ctx.getText().toUpperCase());
  }

  /** Create null order for sort option use from FIRST/LAST token. */
  public static NullOrder createNullOrder(TerminalNode first, TerminalNode last) {
    if (first != null) {
      return NullOrder.NULL_FIRST;
    } else if (last != null) {
      return NullOrder.NULL_LAST;
    } else {
      return null;
    }
  }

  /** Find the all the nodes from a tree that matches the given predicate. */
  public static <T extends Node> List<T> findNodes(Node node, Predicate<Node> condition) {
    List<T> results = new ArrayList<>();
    findNodesHelper(node, condition, results);
    return results;
  }

  private static <T extends Node> void findNodesHelper(
      Node node, Predicate<Node> condition, List<T> results) {
    if (condition.test(node)) {
      results.add((T) node);
    }
    if (node.getChild() != null) {
      for (Node child : node.getChild()) {
        findNodesHelper(child, condition, results);
      }
    }
  }
}
