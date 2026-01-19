/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.executor.QueryType.PPL;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;

/** Build {@link Statement} from PPL Query. */
@RequiredArgsConstructor
public class AstStatementBuilder extends OpenSearchPPLParserBaseVisitor<Statement> {

  private final AstBuilder astBuilder;

  private final StatementBuilderContext context;

  @Override
  public Statement visitPplStatement(OpenSearchPPLParser.PplStatementContext ctx) {
    Query query = new Query(addSelectAll(astBuilder.visit(ctx)), context.getFetchSize(), PPL);
    if (ctx.explainStatement() != null) {
      if (ctx.explainStatement().explainMode() == null) {
        return new Explain(query, PPL);
      } else {
        return new Explain(query, PPL, ctx.explainStatement().explainMode().getText());
      }
    } else {
      return context.isExplain ? new Explain(query, PPL, context.explainMode) : query;
    }
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
    private final String explainMode;
  }

  private UnresolvedPlan addSelectAll(UnresolvedPlan plan) {
    if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
      return plan;
    } else {
      return new Project(ImmutableList.of(AllFields.of())).attach(plan);
    }
  }
}
