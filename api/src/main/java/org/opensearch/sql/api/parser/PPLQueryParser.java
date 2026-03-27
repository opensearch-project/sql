/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/** PPL query parser that produces {@link UnresolvedPlan} as the native parse result. */
@RequiredArgsConstructor
public class PPLQueryParser implements UnifiedQueryParser<UnresolvedPlan> {

  /** Settings containing execution limits and feature flags used by AST builders. */
  private final Settings settings;

  /** Reusable ANTLR-based PPL syntax parser. Stateless and thread-safe. */
  private final PPLSyntaxParser syntaxParser = new PPLSyntaxParser();

  @Override
  public UnresolvedPlan parse(String query) {
    ParseTree cst = syntaxParser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }
}
