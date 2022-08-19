/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ddl.view.ViewConfig.DistributeOption;
import static org.opensearch.sql.ddl.view.ViewConfig.RefreshMode;
import static org.opensearch.sql.ddl.view.ViewDefinition.ViewType;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.CreateMaterializedViewContext;

import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.tree.DataDefinitionPlan;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.ddl.QueryService;
import org.opensearch.sql.ddl.view.CreateMaterializedViewTask;
import org.opensearch.sql.ddl.view.ViewConfig;
import org.opensearch.sql.ddl.view.ViewDefinition;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Build AST for data definition plan.
 */
public class AstDDLBuilder extends OpenSearchSQLParserBaseVisitor<DataDefinitionTask> {

  private final QueryService queryService;

  private final StorageEngine systemCatalog;

  public AstDDLBuilder(QueryService queryService, StorageEngine systemCatalog) {
    this.queryService = queryService;
    this.systemCatalog = systemCatalog;
  }

  public DataDefinitionPlan build(ParseTree tree) {
    return new DataDefinitionPlan(tree.accept(this));
  }

  @Override
  public DataDefinitionTask visitCreateMaterializedView(CreateMaterializedViewContext ctx) {
    return new CreateMaterializedViewTask(
        queryService,
        systemCatalog,
        new ViewDefinition(ctx.tableName().getText(), ViewType.MATERIALIZED_VIEW),
        new ViewConfig(RefreshMode.MANUAL, DistributeOption.EVEN)
    );
  }

  @Override
  protected DataDefinitionTask aggregateResult(DataDefinitionTask aggregate,
                                           DataDefinitionTask nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }
}
