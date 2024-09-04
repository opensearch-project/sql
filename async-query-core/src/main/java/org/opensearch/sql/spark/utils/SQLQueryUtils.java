/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.spark.antlr.parser.FlintSparkSqlExtensionsBaseVisitor;
import org.opensearch.sql.spark.antlr.parser.FlintSparkSqlExtensionsLexer;
import org.opensearch.sql.spark.antlr.parser.FlintSparkSqlExtensionsParser;
import org.opensearch.sql.spark.antlr.parser.FlintSparkSqlExtensionsParser.MaterializedViewQueryContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseLexer;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.IdentifierReferenceContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParserBaseVisitor;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.flint.FlintIndexType;

/**
 * This util class parses spark sql query and provides util functions to identify indexName,
 * tableName and datasourceName.
 */
@UtilityClass
public class SQLQueryUtils {

  public static List<FullyQualifiedTableName> extractFullyQualifiedTableNames(String sqlQuery) {
    SqlBaseParser sqlBaseParser =
        new SqlBaseParser(
            new CommonTokenStream(new SqlBaseLexer(new CaseInsensitiveCharStream(sqlQuery))));
    sqlBaseParser.addErrorListener(new SyntaxAnalysisErrorListener());
    SqlBaseParser.StatementContext statement = sqlBaseParser.statement();
    SparkSqlTableNameVisitor sparkSqlTableNameVisitor = new SparkSqlTableNameVisitor();
    statement.accept(sparkSqlTableNameVisitor);
    return sparkSqlTableNameVisitor.getFullyQualifiedTableNames();
  }

  public static IndexQueryDetails extractIndexDetails(String sqlQuery) {
    FlintSparkSqlExtensionsParser flintSparkSqlExtensionsParser =
        new FlintSparkSqlExtensionsParser(
            new CommonTokenStream(
                new FlintSparkSqlExtensionsLexer(new CaseInsensitiveCharStream(sqlQuery))));
    flintSparkSqlExtensionsParser.addErrorListener(new SyntaxAnalysisErrorListener());
    FlintSparkSqlExtensionsParser.SingleStatementContext singleStatementContext =
        flintSparkSqlExtensionsParser.singleStatement();
    FlintSQLIndexDetailsVisitor flintSQLIndexDetailsVisitor = new FlintSQLIndexDetailsVisitor();
    singleStatementContext.accept(flintSQLIndexDetailsVisitor);
    return flintSQLIndexDetailsVisitor.getIndexQueryDetailsBuilder().build();
  }

  public static boolean isFlintExtensionQuery(String sqlQuery) {
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

  public static List<String> validateSparkSqlQuery(String sqlQuery) {
    SparkSqlValidatorVisitor sparkSqlValidatorVisitor = new SparkSqlValidatorVisitor();
    SqlBaseParser sqlBaseParser =
        new SqlBaseParser(
            new CommonTokenStream(new SqlBaseLexer(new CaseInsensitiveCharStream(sqlQuery))));
    sqlBaseParser.addErrorListener(new SyntaxAnalysisErrorListener());
    try {
      SqlBaseParser.StatementContext statement = sqlBaseParser.statement();
      sparkSqlValidatorVisitor.visit(statement);
      return sparkSqlValidatorVisitor.getValidationErrors();
    } catch (SyntaxCheckException syntaxCheckException) {
      return Collections.emptyList();
    }
  }

  private static class SparkSqlValidatorVisitor extends SqlBaseParserBaseVisitor<Void> {

    @Getter private final List<String> validationErrors = new ArrayList<>();

    @Override
    public Void visitCreateFunction(SqlBaseParser.CreateFunctionContext ctx) {
      validationErrors.add("Creating user-defined functions is not allowed");
      return super.visitCreateFunction(ctx);
    }
  }

  public static class SparkSqlTableNameVisitor extends SqlBaseParserBaseVisitor<Void> {

    @Getter private List<FullyQualifiedTableName> fullyQualifiedTableNames = new LinkedList<>();

    public SparkSqlTableNameVisitor() {}

    @Override
    public Void visitIdentifierReference(IdentifierReferenceContext ctx) {
      fullyQualifiedTableNames.add(new FullyQualifiedTableName(ctx.getText()));
      return super.visitIdentifierReference(ctx);
    }

    @Override
    public Void visitDropTable(SqlBaseParser.DropTableContext ctx) {
      for (ParseTree parseTree : ctx.children) {
        if (parseTree instanceof SqlBaseParser.IdentifierReferenceContext) {
          fullyQualifiedTableNames.add(new FullyQualifiedTableName(parseTree.getText()));
        }
      }
      return super.visitDropTable(ctx);
    }

    @Override
    public Void visitDescribeRelation(SqlBaseParser.DescribeRelationContext ctx) {
      for (ParseTree parseTree : ctx.children) {
        if (parseTree instanceof SqlBaseParser.IdentifierReferenceContext) {
          fullyQualifiedTableNames.add(new FullyQualifiedTableName(parseTree.getText()));
        }
      }
      return super.visitDescribeRelation(ctx);
    }

    // Extract table name for create Table Statement.
    @Override
    public Void visitCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx) {
      for (ParseTree parseTree : ctx.children) {
        if (parseTree instanceof SqlBaseParser.IdentifierReferenceContext) {
          fullyQualifiedTableNames.add(new FullyQualifiedTableName(parseTree.getText()));
        }
      }
      return super.visitCreateTableHeader(ctx);
    }
  }

  public static class FlintSQLIndexDetailsVisitor extends FlintSparkSqlExtensionsBaseVisitor<Void> {

    @Getter private final IndexQueryDetails.IndexQueryDetailsBuilder indexQueryDetailsBuilder;

    public FlintSQLIndexDetailsVisitor() {
      this.indexQueryDetailsBuilder = new IndexQueryDetails.IndexQueryDetailsBuilder();
    }

    @Override
    public Void visitIndexName(FlintSparkSqlExtensionsParser.IndexNameContext ctx) {
      indexQueryDetailsBuilder.indexName(ctx.getText());
      return super.visitIndexName(ctx);
    }

    @Override
    public Void visitTableName(FlintSparkSqlExtensionsParser.TableNameContext ctx) {
      indexQueryDetailsBuilder.fullyQualifiedTableName(new FullyQualifiedTableName(ctx.getText()));
      return super.visitTableName(ctx);
    }

    @Override
    public Void visitCreateSkippingIndexStatement(
        FlintSparkSqlExtensionsParser.CreateSkippingIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.CREATE);
      indexQueryDetailsBuilder.indexType(FlintIndexType.SKIPPING);
      visitPropertyList(ctx.propertyList());
      return super.visitCreateSkippingIndexStatement(ctx);
    }

    @Override
    public Void visitCreateCoveringIndexStatement(
        FlintSparkSqlExtensionsParser.CreateCoveringIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.CREATE);
      indexQueryDetailsBuilder.indexType(FlintIndexType.COVERING);
      visitPropertyList(ctx.propertyList());
      return super.visitCreateCoveringIndexStatement(ctx);
    }

    @Override
    public Void visitCreateMaterializedViewStatement(
        FlintSparkSqlExtensionsParser.CreateMaterializedViewStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.CREATE);
      indexQueryDetailsBuilder.indexType(FlintIndexType.MATERIALIZED_VIEW);
      indexQueryDetailsBuilder.mvName(ctx.mvName.getText());
      visitPropertyList(ctx.propertyList());
      return super.visitCreateMaterializedViewStatement(ctx);
    }

    @Override
    public Void visitDropCoveringIndexStatement(
        FlintSparkSqlExtensionsParser.DropCoveringIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.DROP);
      indexQueryDetailsBuilder.indexType(FlintIndexType.COVERING);
      return super.visitDropCoveringIndexStatement(ctx);
    }

    @Override
    public Void visitDropSkippingIndexStatement(
        FlintSparkSqlExtensionsParser.DropSkippingIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.DROP);
      indexQueryDetailsBuilder.indexType(FlintIndexType.SKIPPING);
      return super.visitDropSkippingIndexStatement(ctx);
    }

    @Override
    public Void visitDropMaterializedViewStatement(
        FlintSparkSqlExtensionsParser.DropMaterializedViewStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.DROP);
      indexQueryDetailsBuilder.indexType(FlintIndexType.MATERIALIZED_VIEW);
      indexQueryDetailsBuilder.mvName(ctx.mvName.getText());
      return super.visitDropMaterializedViewStatement(ctx);
    }

    @Override
    public Void visitVacuumSkippingIndexStatement(
        FlintSparkSqlExtensionsParser.VacuumSkippingIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.VACUUM);
      indexQueryDetailsBuilder.indexType(FlintIndexType.SKIPPING);
      return super.visitVacuumSkippingIndexStatement(ctx);
    }

    @Override
    public Void visitVacuumCoveringIndexStatement(
        FlintSparkSqlExtensionsParser.VacuumCoveringIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.VACUUM);
      indexQueryDetailsBuilder.indexType(FlintIndexType.COVERING);
      return super.visitVacuumCoveringIndexStatement(ctx);
    }

    @Override
    public Void visitVacuumMaterializedViewStatement(
        FlintSparkSqlExtensionsParser.VacuumMaterializedViewStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.VACUUM);
      indexQueryDetailsBuilder.indexType(FlintIndexType.MATERIALIZED_VIEW);
      indexQueryDetailsBuilder.mvName(ctx.mvName.getText());
      return super.visitVacuumMaterializedViewStatement(ctx);
    }

    @Override
    public Void visitDescribeCoveringIndexStatement(
        FlintSparkSqlExtensionsParser.DescribeCoveringIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.DESCRIBE);
      indexQueryDetailsBuilder.indexType(FlintIndexType.COVERING);
      return super.visitDescribeCoveringIndexStatement(ctx);
    }

    @Override
    public Void visitDescribeSkippingIndexStatement(
        FlintSparkSqlExtensionsParser.DescribeSkippingIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.DESCRIBE);
      indexQueryDetailsBuilder.indexType(FlintIndexType.SKIPPING);
      return super.visitDescribeSkippingIndexStatement(ctx);
    }

    @Override
    public Void visitDescribeMaterializedViewStatement(
        FlintSparkSqlExtensionsParser.DescribeMaterializedViewStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.DESCRIBE);
      indexQueryDetailsBuilder.indexType(FlintIndexType.MATERIALIZED_VIEW);
      indexQueryDetailsBuilder.mvName(ctx.mvName.getText());
      return super.visitDescribeMaterializedViewStatement(ctx);
    }

    @Override
    public Void visitShowCoveringIndexStatement(
        FlintSparkSqlExtensionsParser.ShowCoveringIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.SHOW);
      indexQueryDetailsBuilder.indexType(FlintIndexType.COVERING);
      return super.visitShowCoveringIndexStatement(ctx);
    }

    @Override
    public Void visitShowMaterializedViewStatement(
        FlintSparkSqlExtensionsParser.ShowMaterializedViewStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.SHOW);
      indexQueryDetailsBuilder.indexType(FlintIndexType.MATERIALIZED_VIEW);
      return super.visitShowMaterializedViewStatement(ctx);
    }

    @Override
    public Void visitRefreshCoveringIndexStatement(
        FlintSparkSqlExtensionsParser.RefreshCoveringIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.REFRESH);
      indexQueryDetailsBuilder.indexType(FlintIndexType.COVERING);
      return super.visitRefreshCoveringIndexStatement(ctx);
    }

    @Override
    public Void visitRefreshSkippingIndexStatement(
        FlintSparkSqlExtensionsParser.RefreshSkippingIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.REFRESH);
      indexQueryDetailsBuilder.indexType(FlintIndexType.SKIPPING);
      return super.visitRefreshSkippingIndexStatement(ctx);
    }

    @Override
    public Void visitRefreshMaterializedViewStatement(
        FlintSparkSqlExtensionsParser.RefreshMaterializedViewStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.REFRESH);
      indexQueryDetailsBuilder.indexType(FlintIndexType.MATERIALIZED_VIEW);
      indexQueryDetailsBuilder.mvName(ctx.mvName.getText());
      return super.visitRefreshMaterializedViewStatement(ctx);
    }

    @Override
    public Void visitPropertyList(FlintSparkSqlExtensionsParser.PropertyListContext ctx) {
      FlintIndexOptions flintIndexOptions = new FlintIndexOptions();
      if (ctx != null) {
        ctx.property()
            .forEach(
                property ->
                    flintIndexOptions.setOption(
                        removeUnwantedQuotes(propertyKey(property.key).toLowerCase(Locale.ROOT)),
                        removeUnwantedQuotes(
                            propertyValue(property.value).toLowerCase(Locale.ROOT))));
      }
      indexQueryDetailsBuilder.indexOptions(flintIndexOptions);
      return null;
    }

    @Override
    public Void visitAlterCoveringIndexStatement(
        FlintSparkSqlExtensionsParser.AlterCoveringIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.ALTER);
      indexQueryDetailsBuilder.indexType(FlintIndexType.COVERING);
      visitPropertyList(ctx.propertyList());
      return super.visitAlterCoveringIndexStatement(ctx);
    }

    @Override
    public Void visitAlterSkippingIndexStatement(
        FlintSparkSqlExtensionsParser.AlterSkippingIndexStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.ALTER);
      indexQueryDetailsBuilder.indexType(FlintIndexType.SKIPPING);
      visitPropertyList(ctx.propertyList());
      return super.visitAlterSkippingIndexStatement(ctx);
    }

    @Override
    public Void visitAlterMaterializedViewStatement(
        FlintSparkSqlExtensionsParser.AlterMaterializedViewStatementContext ctx) {
      indexQueryDetailsBuilder.indexQueryActionType(IndexQueryActionType.ALTER);
      indexQueryDetailsBuilder.indexType(FlintIndexType.MATERIALIZED_VIEW);
      indexQueryDetailsBuilder.mvName(ctx.mvName.getText());
      visitPropertyList(ctx.propertyList());
      return super.visitAlterMaterializedViewStatement(ctx);
    }

    @Override
    public Void visitMaterializedViewQuery(MaterializedViewQueryContext ctx) {
      int a = ctx.start.getStartIndex();
      int b = ctx.stop.getStopIndex();
      String query = ctx.start.getInputStream().getText(new Interval(a, b));
      indexQueryDetailsBuilder.mvQuery(query);
      return super.visitMaterializedViewQuery(ctx);
    }

    private String propertyKey(FlintSparkSqlExtensionsParser.PropertyKeyContext key) {
      if (key.STRING() != null) {
        return key.STRING().getText();
      } else {
        return key.getText();
      }
    }

    private String propertyValue(FlintSparkSqlExtensionsParser.PropertyValueContext value) {
      if (value.STRING() != null) {
        return value.STRING().getText();
      } else if (value.booleanValue() != null) {
        return value.getText();
      } else {
        return value.getText();
      }
    }

    // TODO: Currently escaping is handled partially.
    // Full implementation should mirror this:
    // https://github.com/apache/spark/blob/v3.5.0/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkParserUtils.scala#L35
    public String removeUnwantedQuotes(String input) {
      return input.replaceAll("^\"|\"$", "");
    }
  }
}
