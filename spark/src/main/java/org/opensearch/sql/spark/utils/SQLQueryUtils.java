/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import lombok.Getter;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.spark.antlr.parser.FlintSparkSqlExtensionsBaseVisitor;
import org.opensearch.sql.spark.antlr.parser.FlintSparkSqlExtensionsLexer;
import org.opensearch.sql.spark.antlr.parser.FlintSparkSqlExtensionsParser;
import org.opensearch.sql.spark.antlr.parser.SqlBaseLexer;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParserBaseVisitor;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;

/**
 * This util class parses spark sql query and provides util functions to identify indexName,
 * tableName and datasourceName.
 */
@UtilityClass
public class SQLQueryUtils {

  // TODO Handle cases where the query has multiple table Names.
  public static FullyQualifiedTableName extractFullyQualifiedTableName(String sqlQuery) {
    SqlBaseParser sqlBaseParser =
        new SqlBaseParser(
            new CommonTokenStream(new SqlBaseLexer(new CaseInsensitiveCharStream(sqlQuery))));
    sqlBaseParser.addErrorListener(new SyntaxAnalysisErrorListener());
    SqlBaseParser.StatementContext statement = sqlBaseParser.statement();
    SparkSqlTableNameVisitor sparkSqlTableNameVisitor = new SparkSqlTableNameVisitor();
    statement.accept(sparkSqlTableNameVisitor);
    return sparkSqlTableNameVisitor.getFullyQualifiedTableName();
  }

  public static IndexDetails extractIndexDetails(String sqlQuery) {
    FlintSparkSqlExtensionsParser flintSparkSqlExtensionsParser =
        new FlintSparkSqlExtensionsParser(
            new CommonTokenStream(
                new FlintSparkSqlExtensionsLexer(new CaseInsensitiveCharStream(sqlQuery))));
    flintSparkSqlExtensionsParser.addErrorListener(new SyntaxAnalysisErrorListener());
    FlintSparkSqlExtensionsParser.StatementContext statementContext =
        flintSparkSqlExtensionsParser.statement();
    FlintSQLIndexDetailsVisitor flintSQLIndexDetailsVisitor = new FlintSQLIndexDetailsVisitor();
    statementContext.accept(flintSQLIndexDetailsVisitor);
    return flintSQLIndexDetailsVisitor.getIndexDetails();
  }

  public static boolean isIndexQuery(String sqlQuery) {
    FlintSparkSqlExtensionsParser flintSparkSqlExtensionsParser =
        new FlintSparkSqlExtensionsParser(
            new CommonTokenStream(
                new FlintSparkSqlExtensionsLexer(new CaseInsensitiveCharStream(sqlQuery))));
    flintSparkSqlExtensionsParser.addErrorListener(new SyntaxAnalysisErrorListener());
    try {
      flintSparkSqlExtensionsParser.statement();
      return true;
    } catch (SyntaxCheckException syntaxCheckException) {
      return false;
    }
  }

  public static class SparkSqlTableNameVisitor extends SqlBaseParserBaseVisitor<Void> {

    @Getter private FullyQualifiedTableName fullyQualifiedTableName;

    public SparkSqlTableNameVisitor() {
      this.fullyQualifiedTableName = new FullyQualifiedTableName();
    }

    @Override
    public Void visitTableName(SqlBaseParser.TableNameContext ctx) {
      fullyQualifiedTableName = new FullyQualifiedTableName(ctx.getText());
      return super.visitTableName(ctx);
    }

    @Override
    public Void visitDropTable(SqlBaseParser.DropTableContext ctx) {
      for (ParseTree parseTree : ctx.children) {
        if (parseTree instanceof SqlBaseParser.IdentifierReferenceContext) {
          fullyQualifiedTableName = new FullyQualifiedTableName(parseTree.getText());
        }
      }
      return super.visitDropTable(ctx);
    }

    @Override
    public Void visitDescribeRelation(SqlBaseParser.DescribeRelationContext ctx) {
      for (ParseTree parseTree : ctx.children) {
        if (parseTree instanceof SqlBaseParser.IdentifierReferenceContext) {
          fullyQualifiedTableName = new FullyQualifiedTableName(parseTree.getText());
        }
      }
      return super.visitDescribeRelation(ctx);
    }

    // Extract table name for create Table Statement.
    @Override
    public Void visitCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx) {
      for (ParseTree parseTree : ctx.children) {
        if (parseTree instanceof SqlBaseParser.IdentifierReferenceContext) {
          fullyQualifiedTableName = new FullyQualifiedTableName(parseTree.getText());
        }
      }
      return super.visitCreateTableHeader(ctx);
    }
  }

  public static class FlintSQLIndexDetailsVisitor extends FlintSparkSqlExtensionsBaseVisitor<Void> {

    @Getter private final IndexDetails indexDetails;

    public FlintSQLIndexDetailsVisitor() {
      this.indexDetails = new IndexDetails();
    }

    @Override
    public Void visitIndexName(FlintSparkSqlExtensionsParser.IndexNameContext ctx) {
      indexDetails.setIndexName(ctx.getText());
      return super.visitIndexName(ctx);
    }

    @Override
    public Void visitTableName(FlintSparkSqlExtensionsParser.TableNameContext ctx) {
      indexDetails.setFullyQualifiedTableName(new FullyQualifiedTableName(ctx.getText()));
      return super.visitTableName(ctx);
    }
  }
}
