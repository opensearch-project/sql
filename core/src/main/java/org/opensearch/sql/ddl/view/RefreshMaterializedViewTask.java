/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import static org.opensearch.sql.ast.dsl.AstDSL.write;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ddl.DataDefinitionTask;

/**
 * Refresh materialized view task.
 */
@RequiredArgsConstructor
@ToString
public class RefreshMaterializedViewTask extends DataDefinitionTask {

  private final QualifiedName viewName;

  private final List<QualifiedName> columns; // TODO: same as below

  private final UnresolvedPlan query; // TODO: read from system table instead

  @Override
  public void execute() {
    /* Read from system table to find refresh query
    QueryResponse resp = queryService.execute(
        project(
            filter(
                relation(".matviews"),
                equalTo(field("viewName"), stringLiteral(viewName))
            ),
            qualifiedName("query")));
    */

    queryService.execute(write(query, viewName, columns));
  }
}
