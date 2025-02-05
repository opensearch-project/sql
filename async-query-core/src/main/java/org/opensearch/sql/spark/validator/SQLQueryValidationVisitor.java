/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AddTableColumnsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AddTablePartitionContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AlterTableAlterColumnContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AlterViewQueryContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AnalyzeContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AnalyzeTablesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.CacheTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ClearCacheContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.CreateNamespaceContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.CreateTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.CreateTableLikeContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.CreateViewContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.CtesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DescribeFunctionContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DescribeNamespaceContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DescribeQueryContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DescribeRelationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DropFunctionContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DropNamespaceContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DropTableColumnsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DropTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DropTablePartitionsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.DropViewContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ErrorCapturingIdentifierContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ErrorCapturingIdentifierExtraContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ExplainContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.FunctionNameContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.HintContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.HiveReplaceColumnsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.InlineTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.InsertIntoReplaceWhereContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.InsertIntoTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.InsertOverwriteDirContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.InsertOverwriteHiveDirContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.InsertOverwriteTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.JoinRelationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.JoinTypeContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.LateralViewContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.LiteralTypeContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.LoadDataContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ManageResourceContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.QueryOrganizationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RecoverPartitionsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RefreshFunctionContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RefreshResourceContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RefreshTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RelationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RenameTableColumnContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RenameTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RenameTablePartitionContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.RepairTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ReplaceTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ResetConfigurationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ResetQuotedConfigurationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SampleContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SetConfigurationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SetNamespaceLocationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SetNamespacePropertiesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SetTableLocationContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SetTableSerDeContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowColumnsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowCreateTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowFunctionsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowNamespacesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowPartitionsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowTableExtendedContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowTablesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowTblPropertiesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ShowViewsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.TableNameContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.TableValuedFunctionContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.TransformClauseContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.TruncateTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.TypeContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.UncacheTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.UnsupportedHiveNativeCommandsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParserBaseVisitor;

/** This visitor validate grammar using GrammarElementValidator */
@AllArgsConstructor
public class SQLQueryValidationVisitor extends SqlBaseParserBaseVisitor<Void> {
  private final GrammarElementValidator grammarElementValidator;

  @Override
  public Void visitCreateFunction(SqlBaseParser.CreateFunctionContext ctx) {
    validateAllowed(SQLGrammarElement.CREATE_FUNCTION);
    return super.visitCreateFunction(ctx);
  }

  @Override
  public Void visitSetNamespaceProperties(SetNamespacePropertiesContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitSetNamespaceProperties(ctx);
  }

  @Override
  public Void visitAddTableColumns(AddTableColumnsContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitAddTableColumns(ctx);
  }

  @Override
  public Void visitAddTablePartition(AddTablePartitionContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitAddTablePartition(ctx);
  }

  @Override
  public Void visitRenameTableColumn(RenameTableColumnContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitRenameTableColumn(ctx);
  }

  @Override
  public Void visitDropTableColumns(DropTableColumnsContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitDropTableColumns(ctx);
  }

  @Override
  public Void visitAlterTableAlterColumn(AlterTableAlterColumnContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitAlterTableAlterColumn(ctx);
  }

  @Override
  public Void visitHiveReplaceColumns(HiveReplaceColumnsContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitHiveReplaceColumns(ctx);
  }

  @Override
  public Void visitSetTableSerDe(SetTableSerDeContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitSetTableSerDe(ctx);
  }

  @Override
  public Void visitRenameTablePartition(RenameTablePartitionContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitRenameTablePartition(ctx);
  }

  @Override
  public Void visitDropTablePartitions(DropTablePartitionsContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitDropTablePartitions(ctx);
  }

  @Override
  public Void visitSetTableLocation(SetTableLocationContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitSetTableLocation(ctx);
  }

  @Override
  public Void visitRecoverPartitions(RecoverPartitionsContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitRecoverPartitions(ctx);
  }

  @Override
  public Void visitSetNamespaceLocation(SetNamespaceLocationContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    return super.visitSetNamespaceLocation(ctx);
  }

  @Override
  public Void visitAlterViewQuery(AlterViewQueryContext ctx) {
    validateAllowed(SQLGrammarElement.ALTER_VIEW);
    return super.visitAlterViewQuery(ctx);
  }

  @Override
  public Void visitRenameTable(RenameTableContext ctx) {
    if (ctx.VIEW() != null) {
      validateAllowed(SQLGrammarElement.ALTER_VIEW);
    } else {
      validateAllowed(SQLGrammarElement.ALTER_NAMESPACE);
    }

    return super.visitRenameTable(ctx);
  }

  @Override
  public Void visitCreateNamespace(CreateNamespaceContext ctx) {
    validateAllowed(SQLGrammarElement.CREATE_NAMESPACE);
    return super.visitCreateNamespace(ctx);
  }

  @Override
  public Void visitCreateTable(CreateTableContext ctx) {
    validateAllowed(SQLGrammarElement.CREATE_NAMESPACE);
    return super.visitCreateTable(ctx);
  }

  @Override
  public Void visitCreateTableLike(CreateTableLikeContext ctx) {
    validateAllowed(SQLGrammarElement.CREATE_NAMESPACE);
    return super.visitCreateTableLike(ctx);
  }

  @Override
  public Void visitReplaceTable(ReplaceTableContext ctx) {
    validateAllowed(SQLGrammarElement.CREATE_NAMESPACE);
    return super.visitReplaceTable(ctx);
  }

  @Override
  public Void visitDropNamespace(DropNamespaceContext ctx) {
    validateAllowed(SQLGrammarElement.DROP_NAMESPACE);
    return super.visitDropNamespace(ctx);
  }

  @Override
  public Void visitDropTable(DropTableContext ctx) {
    validateAllowed(SQLGrammarElement.DROP_NAMESPACE);
    return super.visitDropTable(ctx);
  }

  @Override
  public Void visitCreateView(CreateViewContext ctx) {
    validateAllowed(SQLGrammarElement.CREATE_VIEW);
    return super.visitCreateView(ctx);
  }

  @Override
  public Void visitDropView(DropViewContext ctx) {
    validateAllowed(SQLGrammarElement.DROP_VIEW);
    return super.visitDropView(ctx);
  }

  @Override
  public Void visitDropFunction(DropFunctionContext ctx) {
    validateAllowed(SQLGrammarElement.DROP_FUNCTION);
    return super.visitDropFunction(ctx);
  }

  @Override
  public Void visitRepairTable(RepairTableContext ctx) {
    validateAllowed(SQLGrammarElement.REPAIR_TABLE);
    return super.visitRepairTable(ctx);
  }

  @Override
  public Void visitTruncateTable(TruncateTableContext ctx) {
    validateAllowed(SQLGrammarElement.TRUNCATE_TABLE);
    return super.visitTruncateTable(ctx);
  }

  @Override
  public Void visitInsertOverwriteTable(InsertOverwriteTableContext ctx) {
    validateAllowed(SQLGrammarElement.INSERT);
    return super.visitInsertOverwriteTable(ctx);
  }

  @Override
  public Void visitInsertIntoReplaceWhere(InsertIntoReplaceWhereContext ctx) {
    validateAllowed(SQLGrammarElement.INSERT);
    return super.visitInsertIntoReplaceWhere(ctx);
  }

  @Override
  public Void visitInsertIntoTable(InsertIntoTableContext ctx) {
    validateAllowed(SQLGrammarElement.INSERT);
    return super.visitInsertIntoTable(ctx);
  }

  @Override
  public Void visitInsertOverwriteDir(InsertOverwriteDirContext ctx) {
    validateAllowed(SQLGrammarElement.INSERT);
    return super.visitInsertOverwriteDir(ctx);
  }

  @Override
  public Void visitInsertOverwriteHiveDir(InsertOverwriteHiveDirContext ctx) {
    validateAllowed(SQLGrammarElement.INSERT);
    return super.visitInsertOverwriteHiveDir(ctx);
  }

  @Override
  public Void visitLoadData(LoadDataContext ctx) {
    validateAllowed(SQLGrammarElement.LOAD);
    return super.visitLoadData(ctx);
  }

  @Override
  public Void visitExplain(ExplainContext ctx) {
    validateAllowed(SQLGrammarElement.EXPLAIN);
    return super.visitExplain(ctx);
  }

  @Override
  public Void visitTableName(TableNameContext ctx) {
    String reference = ctx.identifierReference().getText();
    if (isFileReference(reference)) {
      validateAllowed(SQLGrammarElement.FILE);
    }
    return super.visitTableName(ctx);
  }

  private static final String FILE_REFERENCE_PATTERN = "^[a-zA-Z]+\\.`[^`]+`$";

  private boolean isFileReference(String reference) {
    return reference.matches(FILE_REFERENCE_PATTERN);
  }

  @Override
  public Void visitCtes(CtesContext ctx) {
    validateAllowed(SQLGrammarElement.WITH);
    return super.visitCtes(ctx);
  }

  @Override
  public Void visitQueryOrganization(QueryOrganizationContext ctx) {
    if (ctx.CLUSTER() != null) {
      validateAllowed(SQLGrammarElement.CLUSTER_BY);
    } else if (ctx.DISTRIBUTE() != null) {
      validateAllowed(SQLGrammarElement.DISTRIBUTE_BY);
    }
    return super.visitQueryOrganization(ctx);
  }

  @Override
  public Void visitHint(HintContext ctx) {
    validateAllowed(SQLGrammarElement.HINTS);
    return super.visitHint(ctx);
  }

  @Override
  public Void visitInlineTable(InlineTableContext ctx) {
    validateAllowed(SQLGrammarElement.INLINE_TABLE);
    return super.visitInlineTable(ctx);
  }

  @Override
  public Void visitJoinType(JoinTypeContext ctx) {
    if (ctx.CROSS() != null) {
      validateAllowed(SQLGrammarElement.CROSS_JOIN);
    } else if (ctx.LEFT() != null && ctx.SEMI() != null) {
      validateAllowed(SQLGrammarElement.LEFT_SEMI_JOIN);
    } else if (ctx.ANTI() != null) {
      validateAllowed(SQLGrammarElement.LEFT_ANTI_JOIN);
    } else if (ctx.LEFT() != null) {
      validateAllowed(SQLGrammarElement.LEFT_OUTER_JOIN);
    } else if (ctx.RIGHT() != null) {
      validateAllowed(SQLGrammarElement.RIGHT_OUTER_JOIN);
    } else if (ctx.FULL() != null) {
      validateAllowed(SQLGrammarElement.FULL_OUTER_JOIN);
    } else {
      validateAllowed(SQLGrammarElement.INNER_JOIN);
    }
    return super.visitJoinType(ctx);
  }

  @Override
  public Void visitSample(SampleContext ctx) {
    validateAllowed(SQLGrammarElement.TABLESAMPLE);
    return super.visitSample(ctx);
  }

  @Override
  public Void visitTableValuedFunction(TableValuedFunctionContext ctx) {
    validateAllowed(SQLGrammarElement.TABLE_VALUED_FUNCTION);
    return super.visitTableValuedFunction(ctx);
  }

  @Override
  public Void visitLateralView(LateralViewContext ctx) {
    validateAllowed(SQLGrammarElement.LATERAL_VIEW);
    return super.visitLateralView(ctx);
  }

  @Override
  public Void visitRelation(RelationContext ctx) {
    if (ctx.LATERAL() != null) {
      validateAllowed(SQLGrammarElement.LATERAL_SUBQUERY);
    }
    return super.visitRelation(ctx);
  }

  @Override
  public Void visitJoinRelation(JoinRelationContext ctx) {
    if (ctx.LATERAL() != null) {
      validateAllowed(SQLGrammarElement.LATERAL_SUBQUERY);
    }
    return super.visitJoinRelation(ctx);
  }

  @Override
  public Void visitTransformClause(TransformClauseContext ctx) {
    if (ctx.TRANSFORM() != null) {
      validateAllowed(SQLGrammarElement.TRANSFORM);
    }
    return super.visitTransformClause(ctx);
  }

  @Override
  public Void visitManageResource(ManageResourceContext ctx) {
    validateAllowed(SQLGrammarElement.MANAGE_RESOURCE);
    return super.visitManageResource(ctx);
  }

  @Override
  public Void visitAnalyze(AnalyzeContext ctx) {
    validateAllowed(SQLGrammarElement.ANALYZE_TABLE);
    return super.visitAnalyze(ctx);
  }

  @Override
  public Void visitAnalyzeTables(AnalyzeTablesContext ctx) {
    validateAllowed(SQLGrammarElement.ANALYZE_TABLE);
    return super.visitAnalyzeTables(ctx);
  }

  @Override
  public Void visitCacheTable(CacheTableContext ctx) {
    validateAllowed(SQLGrammarElement.CACHE_TABLE);
    return super.visitCacheTable(ctx);
  }

  @Override
  public Void visitClearCache(ClearCacheContext ctx) {
    validateAllowed(SQLGrammarElement.CLEAR_CACHE);
    return super.visitClearCache(ctx);
  }

  @Override
  public Void visitDescribeNamespace(DescribeNamespaceContext ctx) {
    validateAllowed(SQLGrammarElement.DESCRIBE_NAMESPACE);
    return super.visitDescribeNamespace(ctx);
  }

  @Override
  public Void visitDescribeFunction(DescribeFunctionContext ctx) {
    validateAllowed(SQLGrammarElement.DESCRIBE_FUNCTION);
    return super.visitDescribeFunction(ctx);
  }

  @Override
  public Void visitDescribeRelation(DescribeRelationContext ctx) {
    validateAllowed(SQLGrammarElement.DESCRIBE_TABLE);
    return super.visitDescribeRelation(ctx);
  }

  @Override
  public Void visitDescribeQuery(DescribeQueryContext ctx) {
    validateAllowed(SQLGrammarElement.DESCRIBE_QUERY);
    return super.visitDescribeQuery(ctx);
  }

  @Override
  public Void visitRefreshResource(RefreshResourceContext ctx) {
    validateAllowed(SQLGrammarElement.REFRESH_RESOURCE);
    return super.visitRefreshResource(ctx);
  }

  @Override
  public Void visitRefreshTable(RefreshTableContext ctx) {
    validateAllowed(SQLGrammarElement.REFRESH_TABLE);
    return super.visitRefreshTable(ctx);
  }

  @Override
  public Void visitRefreshFunction(RefreshFunctionContext ctx) {
    validateAllowed(SQLGrammarElement.REFRESH_FUNCTION);
    return super.visitRefreshFunction(ctx);
  }

  @Override
  public Void visitResetConfiguration(ResetConfigurationContext ctx) {
    validateAllowed(SQLGrammarElement.RESET);
    return super.visitResetConfiguration(ctx);
  }

  @Override
  public Void visitResetQuotedConfiguration(ResetQuotedConfigurationContext ctx) {
    validateAllowed(SQLGrammarElement.RESET);
    return super.visitResetQuotedConfiguration(ctx);
  }

  @Override
  public Void visitSetConfiguration(SetConfigurationContext ctx) {
    validateAllowed(SQLGrammarElement.SET);
    return super.visitSetConfiguration(ctx);
  }

  @Override
  public Void visitShowColumns(ShowColumnsContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_COLUMNS);
    return super.visitShowColumns(ctx);
  }

  @Override
  public Void visitShowCreateTable(ShowCreateTableContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_CREATE_TABLE);
    return super.visitShowCreateTable(ctx);
  }

  @Override
  public Void visitShowNamespaces(ShowNamespacesContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_NAMESPACES);
    return super.visitShowNamespaces(ctx);
  }

  @Override
  public Void visitShowFunctions(ShowFunctionsContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_FUNCTIONS);
    return super.visitShowFunctions(ctx);
  }

  @Override
  public Void visitShowPartitions(ShowPartitionsContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_PARTITIONS);
    return super.visitShowPartitions(ctx);
  }

  @Override
  public Void visitShowTableExtended(ShowTableExtendedContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_TABLE_EXTENDED);
    return super.visitShowTableExtended(ctx);
  }

  @Override
  public Void visitShowTables(ShowTablesContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_TABLES);
    return super.visitShowTables(ctx);
  }

  @Override
  public Void visitShowTblProperties(ShowTblPropertiesContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_TBLPROPERTIES);
    return super.visitShowTblProperties(ctx);
  }

  @Override
  public Void visitShowViews(ShowViewsContext ctx) {
    validateAllowed(SQLGrammarElement.SHOW_VIEWS);
    return super.visitShowViews(ctx);
  }

  @Override
  public Void visitUncacheTable(UncacheTableContext ctx) {
    validateAllowed(SQLGrammarElement.UNCACHE_TABLE);
    return super.visitUncacheTable(ctx);
  }

  @Override
  public Void visitFunctionName(FunctionNameContext ctx) {
    validateFunctionAllowed(ctx.qualifiedName().getText());
    return super.visitFunctionName(ctx);
  }

  private void validateFunctionAllowed(String function) {
    String functionLower = function.toLowerCase();
    FunctionType type = FunctionType.fromFunctionName(functionLower);
    switch (type) {
      case MAP:
        validateAllowed(SQLGrammarElement.MAP_FUNCTIONS, functionLower);
        break;
      case BITWISE:
        validateAllowed(SQLGrammarElement.BITWISE_FUNCTIONS, functionLower);
        break;
      case CSV:
        validateAllowed(SQLGrammarElement.CSV_FUNCTIONS, functionLower);
        break;
      case MISC:
        validateAllowed(SQLGrammarElement.MISC_FUNCTIONS, functionLower);
        break;
      case GENERATOR:
        validateAllowed(SQLGrammarElement.GENERATOR_FUNCTIONS, functionLower);
        break;
      case UNCATEGORIZED:
        validateAllowed(SQLGrammarElement.UNCATEGORIZED_FUNCTIONS, functionLower);
        break;
      case UDF:
        validateAllowed(SQLGrammarElement.UDF, functionLower);
        break;
    }
  }

  private void validateAllowed(SQLGrammarElement element) {
    if (!grammarElementValidator.isValid(element)) {
      throw new IllegalArgumentException(element + " is not allowed.");
    }
  }

  private void validateAllowed(SQLGrammarElement element, String detail) {
    if (!grammarElementValidator.isValid(element)) {
      throw new IllegalArgumentException(String.format("%s (%s) is not allowed.", element, detail));
    }
  }

  @Override
  public Void visitErrorCapturingIdentifier(ErrorCapturingIdentifierContext ctx) {
    ErrorCapturingIdentifierExtraContext extra = ctx.errorCapturingIdentifierExtra();
    if (extra.children != null) {
      throw new IllegalArgumentException("Invalid identifier: " + ctx.getText());
    }
    return super.visitErrorCapturingIdentifier(ctx);
  }

  @Override
  public Void visitLiteralType(LiteralTypeContext ctx) {
    if (ctx.unsupportedType != null) {
      throw new IllegalArgumentException("Unsupported typed literal: " + ctx.getText());
    }
    return super.visitLiteralType(ctx);
  }

  @Override
  public Void visitType(TypeContext ctx) {
    if (ctx.unsupportedType != null) {
      throw new IllegalArgumentException("Unsupported data type: " + ctx.getText());
    }
    return super.visitType(ctx);
  }

  @Override
  public Void visitUnsupportedHiveNativeCommands(UnsupportedHiveNativeCommandsContext ctx) {
    throw new IllegalArgumentException("Unsupported command.");
  }
}
