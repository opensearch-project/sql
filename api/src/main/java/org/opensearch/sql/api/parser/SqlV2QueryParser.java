/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;

/** SQL query parser that produces {@link UnresolvedPlan} using the V2 ANTLR grammar. */
public class SqlV2QueryParser implements UnifiedQueryParser<UnresolvedPlan> {

  /** Reusable ANTLR-based SQL syntax parser. Stateless and thread-safe. */
  private final SQLSyntaxParser syntaxParser = new SQLSyntaxParser();

  @Override
  public UnresolvedPlan parse(String query) {
    ParseTree cst = syntaxParser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new AstBuilder(query), AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }
}
