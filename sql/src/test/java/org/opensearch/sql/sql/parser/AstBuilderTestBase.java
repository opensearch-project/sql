/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;

public class AstBuilderTestBase {
  /**
   * SQL syntax parser that helps prepare parse tree as AstBuilder input.
   */
  private final SQLSyntaxParser parser = new SQLSyntaxParser();

  protected UnresolvedPlan buildAST(String query) {
    ParseTree parseTree = parser.parse(query);
    return parseTree.accept(new AstBuilder(query));
  }
}
