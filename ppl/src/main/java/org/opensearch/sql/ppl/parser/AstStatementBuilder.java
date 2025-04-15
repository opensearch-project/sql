/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.executor.QueryType.PPL;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;

/** Build {@link Statement} from PPL Query. */
@RequiredArgsConstructor
public class AstStatementBuilder extends OpenSearchPPLParserBaseVisitor<Statement> {

  private final AstBuilder astBuilder;

  private final StatementBuilderContext context;

  @Override
  public Statement visitDmlStatement(OpenSearchPPLParser.DmlStatementContext ctx) {
    Query query = new Query(addSelectAll(astBuilder.visit(ctx)), context.getFetchSize(), PPL);
    return context.isExplain ? new Explain(query, PPL, context.format) : query;
  }

  @Override
  protected Statement aggregateResult(Statement aggregate, Statement nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  @Data
  @Builder
  public static class StatementBuilderContext {
    private final boolean isExplain;
    private final int fetchSize;
    private final String format;
  }

  private UnresolvedPlan addSelectAll(UnresolvedPlan plan) {
    return AllFields.of().apply(plan);
  }
}
