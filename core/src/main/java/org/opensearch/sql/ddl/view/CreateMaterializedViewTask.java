/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import static org.opensearch.sql.ast.dsl.AstDSL.createTable;
import static org.opensearch.sql.ast.dsl.AstDSL.insert;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.values;

import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ddl.QueryService;
import org.opensearch.sql.utils.SystemIndexUtils;

/**
 * Create materialized view task.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public class CreateMaterializedViewTask extends ViewDataDefinitionTask {

  private final ViewDefinition definition;

  private final ViewConfig config;

  public CreateMaterializedViewTask(QueryService queryService,
                                    ViewDefinition definition,
                                    ViewConfig config) {
    super(queryService);
    this.definition = definition;
    this.config = config;
  }

  @Override
  public void execute() {
    // 1.Create mv index
    UnresolvedPlan createViewTable =
        createTable(
            qualifiedName(definition.getViewName()),
            null);
    queryService.execute(createViewTable);

    // 2.Add mv info to system metadata if not exist
    String viewMetaTable = SystemIndexUtils.systemTable("sql-views").getTableName();
    UnresolvedPlan insertViewMeta =
        insert(
            values(
                Arrays.asList(
                    stringLiteral(definition.getViewName()),
                    stringLiteral(definition.getViewType().toString()),
                    stringLiteral(definition.getQuery().toString()))),
            qualifiedName(viewMetaTable),
            Arrays.asList(
                qualifiedName("viewName"),
                qualifiedName("viewType"),
                qualifiedName("query")));
    queryService.execute(insertViewMeta);

    // 3.Trigger view refresh
    queryService.execute(definition.getQuery());
  }

}
