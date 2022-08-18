/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ddl.view.ViewConfig.DistributeOption;
import static org.opensearch.sql.ddl.view.ViewConfig.RefreshMode;
import static org.opensearch.sql.ddl.view.ViewDefinition.ViewType;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.CreateMaterializedViewContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.CreateTableContext;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.tree.DataDefinitionPlan;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.ddl.QueryService;
import org.opensearch.sql.ddl.table.CreateTableTask;
import org.opensearch.sql.ddl.table.Schema;
import org.opensearch.sql.ddl.view.CreateMaterializedViewTask;
import org.opensearch.sql.ddl.view.ViewConfig;
import org.opensearch.sql.ddl.view.ViewDefinition;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Data definition task factory.
 */
@RequiredArgsConstructor
public class DataDefinitionTaskFactory extends OpenSearchSQLParserBaseVisitor<DataDefinitionTask> {

  private final QueryService queryService;

  private final StorageEngine systemCatalog;

  public DataDefinitionPlan build(ParseTree tree) {
    return new DataDefinitionPlan(tree.accept(this));
  }

  @Override
  public DataDefinitionTask visitCreateTable(CreateTableContext ctx) {
    return new CreateTableTask(
        systemCatalog, // ?
        new Schema(ctx.tableName().getText())
    );
  }

  @Override
  public DataDefinitionTask visitCreateMaterializedView(CreateMaterializedViewContext ctx) {
    return new CreateMaterializedViewTask(
        queryService,
        new ViewDefinition(ctx.viewName().getText(), ViewType.MATERIALIZED_VIEW),
        new ViewConfig(RefreshMode.MANUAL, DistributeOption.EVEN)
    );
  }
}
