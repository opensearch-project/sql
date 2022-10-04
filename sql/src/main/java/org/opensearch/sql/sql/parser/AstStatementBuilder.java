/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.sql.parser;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.statement.CreateTable;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;

@RequiredArgsConstructor
public class AstStatementBuilder extends OpenSearchSQLParserBaseVisitor<Statement> {

  private final AstBuilder astBuilder;

  private final StatementBuilderContext context;

  @Override
  public Statement visitDmlStatement(OpenSearchSQLParser.DmlStatementContext ctx) {
    if (context.isExplain) {
      return new Explain(astBuilder.visit(ctx));
    } else {
      return new Query(astBuilder.visit(ctx));
    }
  }

  @Override
  public Statement visitAdminStatement(OpenSearchSQLParser.AdminStatementContext ctx) {
    if (context.isExplain) {
      return new Explain(astBuilder.visit(ctx));
    } else {
      return new Query(astBuilder.visit(ctx));
    }
  }

  @Override
  public Statement visitDdlStatement(OpenSearchSQLParser.DdlStatementContext ctx) {
    return new CreateTable(new AstDDLBuilder(astBuilder).visit(ctx));
  }

  @Override
  protected Statement aggregateResult(Statement aggregate, Statement nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  @Data
  @Builder
  public static class StatementBuilderContext {
    private final boolean isExplain;
  }
}
