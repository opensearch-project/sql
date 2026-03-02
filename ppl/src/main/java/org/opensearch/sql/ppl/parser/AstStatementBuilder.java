/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.executor.QueryType.PPL;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Head;
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
    UnresolvedPlan rawPlan = astBuilder.visit(ctx);
    if (context.getFetchSize() > 0 && !containsHead(rawPlan)) {
      rawPlan = new Head(context.getFetchSize(), 0).attach(rawPlan);
    }
    UnresolvedPlan plan = addSelectAll(rawPlan);
    Query query = new Query(plan, 0, PPL);
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

    /**
     * Maximum number of results to return. 0 means use system default. Unlike SQL's fetch_size
     * which enables cursor-based pagination, PPL's fetch_size limits the response to N rows without
     * cursor support.
     */
    private final int fetchSize;

    private final String format;
    private final String explainMode;
  }

  /**
   * Checks if the main pipeline contains a {@link Head} node by walking the first-child chain. Only
   * the main pipeline is checked — subqueries in joins or nested structures are not traversed. When
   * the user's query already includes an explicit {@code head} command, we should not inject an
   * additional Head for fetch_size so that the user's explicit limit takes precedence.
   */
  private boolean containsHead(UnresolvedPlan plan) {
    UnresolvedPlan current = plan;
    while (current != null) {
      if (current instanceof Head) {
        return true;
      }
      List<? extends Node> children = current.getChild();
      if (children.isEmpty() || !(children.get(0) instanceof UnresolvedPlan)) {
        break;
      }
      current = (UnresolvedPlan) children.get(0);
    }
    return false;
  }

  private UnresolvedPlan addSelectAll(UnresolvedPlan plan) {
    if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
      return plan;
    } else {
      return new Project(ImmutableList.of(AllFields.of())).attach(plan);
    }
  }
}
