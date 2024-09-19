/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AddTableColumnsContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AddTablePartitionContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AlterClusterByContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AlterTableAlterColumnContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AlterViewQueryContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AlterViewSchemaBindingContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AnalyzeContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.AnalyzeTablesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.CacheTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ClearCacheContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.ClusterBySpecContext;
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
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.UncacheTableContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.UnsetNamespacePropertiesContext;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParserBaseVisitor;

@AllArgsConstructor
public class SQLQueryValidationVisitor extends SqlBaseParserBaseVisitor<Void> {
  private final GrammarElementValidator grammarElementValidator;

  @Override
  public Void visitCreateFunction(SqlBaseParser.CreateFunctionContext ctx) {
    validateAllowed(GrammarElement.CREATE_FUNCTION);
    return super.visitCreateFunction(ctx);
  }

  @Override
  public Void visitSetNamespaceProperties(SetNamespacePropertiesContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitSetNamespaceProperties(ctx);
  }

  @Override
  public Void visitUnsetNamespaceProperties(UnsetNamespacePropertiesContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitUnsetNamespaceProperties(ctx);
  }

  @Override
  public Void visitAddTableColumns(AddTableColumnsContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitAddTableColumns(ctx);
  }

  @Override
  public Void visitAddTablePartition(AddTablePartitionContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitAddTablePartition(ctx);
  }

  @Override
  public Void visitRenameTableColumn(RenameTableColumnContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitRenameTableColumn(ctx);
  }

  @Override
  public Void visitDropTableColumns(DropTableColumnsContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitDropTableColumns(ctx);
  }

  @Override
  public Void visitAlterTableAlterColumn(AlterTableAlterColumnContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitAlterTableAlterColumn(ctx);
  }

  @Override
  public Void visitHiveReplaceColumns(HiveReplaceColumnsContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitHiveReplaceColumns(ctx);
  }

  @Override
  public Void visitSetTableSerDe(SetTableSerDeContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitSetTableSerDe(ctx);
  }

  @Override
  public Void visitRenameTablePartition(RenameTablePartitionContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitRenameTablePartition(ctx);
  }

  @Override
  public Void visitDropTablePartitions(DropTablePartitionsContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitDropTablePartitions(ctx);
  }

  @Override
  public Void visitSetTableLocation(SetTableLocationContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitSetTableLocation(ctx);
  }

  @Override
  public Void visitRecoverPartitions(RecoverPartitionsContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitRecoverPartitions(ctx);
  }

  @Override
  public Void visitAlterClusterBy(AlterClusterByContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitAlterClusterBy(ctx);
  }

  @Override
  public Void visitSetNamespaceLocation(SetNamespaceLocationContext ctx) {
    validateAllowed(GrammarElement.ALTER_NAMESPACE);
    return super.visitSetNamespaceLocation(ctx);
  }

  @Override
  public Void visitAlterViewQuery(AlterViewQueryContext ctx) {
    validateAllowed(GrammarElement.ALTER_VIEW);
    return super.visitAlterViewQuery(ctx);
  }

  @Override
  public Void visitAlterViewSchemaBinding(AlterViewSchemaBindingContext ctx) {
    validateAllowed(GrammarElement.ALTER_VIEW);
    return super.visitAlterViewSchemaBinding(ctx);
  }

  @Override
  public Void visitRenameTable(RenameTableContext ctx) {
    if (ctx.VIEW() != null) {
      validateAllowed(GrammarElement.ALTER_VIEW);
    } else {
      validateAllowed(GrammarElement.ALTER_NAMESPACE);
    }

    return super.visitRenameTable(ctx);
  }

  @Override
  public Void visitCreateNamespace(CreateNamespaceContext ctx) {
    validateAllowed(GrammarElement.CREATE_NAMESPACE);
    return super.visitCreateNamespace(ctx);
  }

  @Override
  public Void visitCreateTable(CreateTableContext ctx) {
    validateAllowed(GrammarElement.CREATE_NAMESPACE);
    return super.visitCreateTable(ctx);
  }

  @Override
  public Void visitCreateTableLike(CreateTableLikeContext ctx) {
    validateAllowed(GrammarElement.CREATE_NAMESPACE);
    return super.visitCreateTableLike(ctx);
  }

  @Override
  public Void visitReplaceTable(ReplaceTableContext ctx) {
    validateAllowed(GrammarElement.CREATE_NAMESPACE);
    return super.visitReplaceTable(ctx);
  }

  @Override
  public Void visitDropNamespace(DropNamespaceContext ctx) {
    validateAllowed(GrammarElement.DROP_NAMESPACE);
    return super.visitDropNamespace(ctx);
  }

  @Override
  public Void visitDropTable(DropTableContext ctx) {
    validateAllowed(GrammarElement.DROP_NAMESPACE);
    return super.visitDropTable(ctx);
  }

  @Override
  public Void visitCreateView(CreateViewContext ctx) {
    validateAllowed(GrammarElement.CREATE_VIEW);
    return super.visitCreateView(ctx);
  }

  @Override
  public Void visitDropView(DropViewContext ctx) {
    validateAllowed(GrammarElement.DROP_VIEW);
    return super.visitDropView(ctx);
  }

  @Override
  public Void visitDropFunction(DropFunctionContext ctx) {
    validateAllowed(GrammarElement.DROP_FUNCTION);
    return super.visitDropFunction(ctx);
  }

  @Override
  public Void visitRepairTable(RepairTableContext ctx) {
    validateAllowed(GrammarElement.REPAIR_TABLE);
    return super.visitRepairTable(ctx);
  }

  @Override
  public Void visitTruncateTable(TruncateTableContext ctx) {
    validateAllowed(GrammarElement.TRUNCATE_TABLE);
    return super.visitTruncateTable(ctx);
  }

  @Override
  public Void visitInsertOverwriteTable(InsertOverwriteTableContext ctx) {
    validateAllowed(GrammarElement.INSERT);
    return super.visitInsertOverwriteTable(ctx);
  }

  @Override
  public Void visitInsertIntoReplaceWhere(InsertIntoReplaceWhereContext ctx) {
    validateAllowed(GrammarElement.INSERT);
    return super.visitInsertIntoReplaceWhere(ctx);
  }

  @Override
  public Void visitInsertIntoTable(InsertIntoTableContext ctx) {
    validateAllowed(GrammarElement.INSERT);
    return super.visitInsertIntoTable(ctx);
  }

  @Override
  public Void visitInsertOverwriteDir(InsertOverwriteDirContext ctx) {
    validateAllowed(GrammarElement.INSERT);
    return super.visitInsertOverwriteDir(ctx);
  }

  @Override
  public Void visitInsertOverwriteHiveDir(InsertOverwriteHiveDirContext ctx) {
    validateAllowed(GrammarElement.INSERT);
    return super.visitInsertOverwriteHiveDir(ctx);
  }

  @Override
  public Void visitLoadData(LoadDataContext ctx) {
    validateAllowed(GrammarElement.LOAD);
    return super.visitLoadData(ctx);
  }

  @Override
  public Void visitExplain(ExplainContext ctx) {
    validateAllowed(GrammarElement.EXPLAIN);
    return super.visitExplain(ctx);
  }

  @Override
  public Void visitTableName(TableNameContext ctx) {
    String reference = ctx.identifierReference().getText();
    if (isFileReference(reference)) {
      validateAllowed(GrammarElement.FILE);
    }
    return super.visitTableName(ctx);
  }

  private static final String FILE_REFERENCE_PATTERN = "^[a-zA-Z]+\\.`[^`]+`$";

  private boolean isFileReference(String reference) {
    return reference.matches(FILE_REFERENCE_PATTERN);
  }

  @Override
  public Void visitCtes(CtesContext ctx) {
    validateAllowed(GrammarElement.WITH);
    return super.visitCtes(ctx);
  }

  @Override
  public Void visitClusterBySpec(ClusterBySpecContext ctx) {
    validateAllowed(GrammarElement.CLUSTER_BY);
    return super.visitClusterBySpec(ctx);
  }

  @Override
  public Void visitQueryOrganization(QueryOrganizationContext ctx) {
    if (ctx.CLUSTER() != null) {
      validateAllowed(GrammarElement.CLUSTER_BY);
    } else if (ctx.DISTRIBUTE() != null) {
      validateAllowed(GrammarElement.DISTRIBUTE_BY);
    }
    return super.visitQueryOrganization(ctx);
  }

  @Override
  public Void visitHint(HintContext ctx) {
    validateAllowed(GrammarElement.HINTS);
    return super.visitHint(ctx);
  }

  @Override
  public Void visitInlineTable(InlineTableContext ctx) {
    validateAllowed(GrammarElement.INLINE_TABLE);
    return super.visitInlineTable(ctx);
  }

  @Override
  public Void visitJoinType(JoinTypeContext ctx) {
    if (ctx.CROSS() != null) {
      validateAllowed(GrammarElement.CROSS_JOIN);
    } else if (ctx.LEFT() != null && ctx.SEMI() != null) {
      validateAllowed(GrammarElement.LEFT_SEMI_JOIN);
    } else if (ctx.ANTI() != null) {
      validateAllowed(GrammarElement.LEFT_ANTI_JOIN);
    } else if (ctx.LEFT() != null) {
      validateAllowed(GrammarElement.LEFT_OUTER_JOIN);
    } else if (ctx.RIGHT() != null) {
      validateAllowed(GrammarElement.RIGHT_OUTER_JOIN);
    } else if (ctx.FULL() != null) {
      validateAllowed(GrammarElement.FULL_OUTER_JOIN);
    } else {
      validateAllowed(GrammarElement.INNER_JOIN);
    }
    return super.visitJoinType(ctx);
  }

  @Override
  public Void visitSample(SampleContext ctx) {
    validateAllowed(GrammarElement.TABLESAMPLE);
    return super.visitSample(ctx);
  }

  @Override
  public Void visitTableValuedFunction(TableValuedFunctionContext ctx) {
    validateAllowed(GrammarElement.TABLE_VALUED_FUNCTION);
    return super.visitTableValuedFunction(ctx);
  }

  @Override
  public Void visitLateralView(LateralViewContext ctx) {
    validateAllowed(GrammarElement.LATERAL_VIEW);
    return super.visitLateralView(ctx);
  }

  @Override
  public Void visitRelation(RelationContext ctx) {
    if (ctx.LATERAL() != null) {
      validateAllowed(GrammarElement.LATERAL_SUBQUERY);
    }
    return super.visitRelation(ctx);
  }

  @Override
  public Void visitJoinRelation(JoinRelationContext ctx) {
    if (ctx.LATERAL() != null) {
      validateAllowed(GrammarElement.LATERAL_SUBQUERY);
    }
    return super.visitJoinRelation(ctx);
  }

  @Override
  public Void visitTransformClause(TransformClauseContext ctx) {
    if (ctx.TRANSFORM() != null) {
      validateAllowed(GrammarElement.TRANSFORM);
    }
    return super.visitTransformClause(ctx);
  }

  @Override
  public Void visitManageResource(ManageResourceContext ctx) {
    validateAllowed(GrammarElement.MANAGE_RESOURCE);
    return super.visitManageResource(ctx);
  }

  @Override
  public Void visitAnalyze(AnalyzeContext ctx) {
    validateAllowed(GrammarElement.ANALYZE_TABLE);
    return super.visitAnalyze(ctx);
  }

  @Override
  public Void visitAnalyzeTables(AnalyzeTablesContext ctx) {
    validateAllowed(GrammarElement.ANALYZE_TABLE);
    return super.visitAnalyzeTables(ctx);
  }

  @Override
  public Void visitCacheTable(CacheTableContext ctx) {
    validateAllowed(GrammarElement.CACHE_TABLE);
    return super.visitCacheTable(ctx);
  }

  @Override
  public Void visitClearCache(ClearCacheContext ctx) {
    validateAllowed(GrammarElement.CLEAR_CACHE);
    return super.visitClearCache(ctx);
  }

  @Override
  public Void visitDescribeNamespace(DescribeNamespaceContext ctx) {
    validateAllowed(GrammarElement.DESCRIBE_NAMESPACE);
    return super.visitDescribeNamespace(ctx);
  }

  @Override
  public Void visitDescribeFunction(DescribeFunctionContext ctx) {
    validateAllowed(GrammarElement.DESCRIBE_FUNCTION);
    return super.visitDescribeFunction(ctx);
  }

  @Override
  public Void visitDescribeRelation(DescribeRelationContext ctx) {
    validateAllowed(GrammarElement.DESCRIBE_TABLE);
    return super.visitDescribeRelation(ctx);
  }

  @Override
  public Void visitDescribeQuery(DescribeQueryContext ctx) {
    validateAllowed(GrammarElement.DESCRIBE_QUERY);
    return super.visitDescribeQuery(ctx);
  }

  @Override
  public Void visitRefreshResource(RefreshResourceContext ctx) {
    validateAllowed(GrammarElement.REFRESH_RESOURCE);
    return super.visitRefreshResource(ctx);
  }

  @Override
  public Void visitRefreshTable(RefreshTableContext ctx) {
    validateAllowed(GrammarElement.REFRESH_TABLE);
    return super.visitRefreshTable(ctx);
  }

  @Override
  public Void visitRefreshFunction(RefreshFunctionContext ctx) {
    validateAllowed(GrammarElement.REFRESH_FUNCTION);
    return super.visitRefreshFunction(ctx);
  }

  @Override
  public Void visitResetConfiguration(ResetConfigurationContext ctx) {
    validateAllowed(GrammarElement.RESET);
    return super.visitResetConfiguration(ctx);
  }

  @Override
  public Void visitResetQuotedConfiguration(ResetQuotedConfigurationContext ctx) {
    validateAllowed(GrammarElement.RESET);
    return super.visitResetQuotedConfiguration(ctx);
  }

  @Override
  public Void visitSetConfiguration(SetConfigurationContext ctx) {
    validateAllowed(GrammarElement.SET);
    return super.visitSetConfiguration(ctx);
  }

  @Override
  public Void visitShowColumns(ShowColumnsContext ctx) {
    validateAllowed(GrammarElement.SHOW_COLUMNS);
    return super.visitShowColumns(ctx);
  }

  @Override
  public Void visitShowCreateTable(ShowCreateTableContext ctx) {
    validateAllowed(GrammarElement.SHOW_CREATE_TABLE);
    return super.visitShowCreateTable(ctx);
  }

  @Override
  public Void visitShowNamespaces(ShowNamespacesContext ctx) {
    validateAllowed(GrammarElement.SHOW_NAMESPACES);
    return super.visitShowNamespaces(ctx);
  }

  @Override
  public Void visitShowFunctions(ShowFunctionsContext ctx) {
    validateAllowed(GrammarElement.SHOW_FUNCTIONS);
    return super.visitShowFunctions(ctx);
  }

  @Override
  public Void visitShowPartitions(ShowPartitionsContext ctx) {
    validateAllowed(GrammarElement.SHOW_PARTITIONS);
    return super.visitShowPartitions(ctx);
  }

  @Override
  public Void visitShowTableExtended(ShowTableExtendedContext ctx) {
    validateAllowed(GrammarElement.SHOW_TABLE_EXTENDED);
    return super.visitShowTableExtended(ctx);
  }

  @Override
  public Void visitShowTables(ShowTablesContext ctx) {
    validateAllowed(GrammarElement.SHOW_TABLES);
    return super.visitShowTables(ctx);
  }

  @Override
  public Void visitShowTblProperties(ShowTblPropertiesContext ctx) {
    validateAllowed(GrammarElement.SHOW_TBLPROPERTIES);
    return super.visitShowTblProperties(ctx);
  }

  @Override
  public Void visitShowViews(ShowViewsContext ctx) {
    validateAllowed(GrammarElement.SHOW_VIEWS);
    return super.visitShowViews(ctx);
  }

  @Override
  public Void visitUncacheTable(UncacheTableContext ctx) {
    validateAllowed(GrammarElement.UNCACHE_TABLE);
    return super.visitUncacheTable(ctx);
  }

  @Override
  public Void visitFunctionName(FunctionNameContext ctx) {
    validateFunctionAllowed(ctx.qualifiedName().getText());
    return super.visitFunctionName(ctx);
  }

  private void validateFunctionAllowed(String function) {
    FunctionType type = FunctionType.fromFunctionName(function.toLowerCase());
    switch (type) {
      case MAP:
        validateAllowed(GrammarElement.MAP_FUNCTIONS);
        break;
      case CSV:
        validateAllowed(GrammarElement.CSV_FUNCTIONS);
        break;
      case MISC:
        validateAllowed(GrammarElement.MISC_FUNCTIONS);
        break;
      case UDF:
        validateAllowed(GrammarElement.UDF);
        break;
    }
  }

  private void validateAllowed(GrammarElement element) {
    if (!grammarElementValidator.isValid(element)) {
      throw new IllegalArgumentException(element + " is not allowed.");
    }
  }
}
