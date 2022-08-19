/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import static org.opensearch.sql.ast.dsl.AstDSL.createTable;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.ast.dsl.AstDSL.write;

import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ddl.Column;
import org.opensearch.sql.ddl.DataDefinitionTask;

/**
 * Create materialized view task.
 */
@RequiredArgsConstructor
@ToString
public class CreateMaterializedViewTask extends DataDefinitionTask {

  private final ViewDefinition definition;

  private final ViewConfig config;

  @Override
  public void execute() {
    // 1.Create mv index
    UnresolvedPlan createViewTable =
        createTable(
            qualifiedName(definition.getViewName()),
            definition.getColumns().toArray(new Column[0])
        );
    queryService.execute(createViewTable);

    // 2.Add mv info to system metadata if not exist
    UnresolvedPlan insertViewMeta =
        write(
            values(
                Arrays.asList(
                    stringLiteral(definition.getViewName()),
                    stringLiteral(definition.getViewType().toString()),
                    stringLiteral(definition.getQuery().toString()))),
            qualifiedName("_ODFE_SYS_TABLE_META.views"),
            Arrays.asList(
                qualifiedName("viewName"),
                qualifiedName("viewType"),
                qualifiedName("query")));
    queryService.execute(insertViewMeta);

    // 3.Trigger view refresh
    //queryService.execute(definition.getQuery());
  }
}