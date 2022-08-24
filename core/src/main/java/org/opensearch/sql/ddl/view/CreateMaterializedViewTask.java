/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import static org.opensearch.sql.ast.dsl.AstDSL.createTable;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.refreshMaterializedView;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.ast.dsl.AstDSL.write;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.expression.QualifiedName;
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
    try {
      // 1.Create mv index
      UnresolvedPlan createViewTable =
          createTable(
              qualifiedName(definition.getViewName()),
              definition.getColumns().toArray(new Column[0])
          );
      queryService.execute(createViewTable);

      ObjectMapper OBJECT_MAPPER = new ObjectMapper();
      NodeSerializer serializer = new NodeSerializer();

      List<String> columnNames = definition.getColumns().stream()
          .map(col -> col.getName())
          .collect(Collectors.toList());

      // 2.Add mv info to system metadata if not exist
      UnresolvedPlan insertViewMeta =
          write(
              values(
                  Arrays.asList(
                      stringLiteral(definition.getViewName()),
                      stringLiteral(definition.getViewType().toString()),
                      stringLiteral(serializer.serializeNode(definition.getQuery())),
                      stringLiteral(OBJECT_MAPPER.writeValueAsString(columnNames)))),
              //qualifiedName("_ODFE_SYS_TABLE_META.views"),
              qualifiedName(".matviews"),
              Arrays.asList(
                  qualifiedName("viewName"),
                  qualifiedName("viewType"),
                  qualifiedName("query"),
                  qualifiedName("columns")));
      queryService.execute(insertViewMeta);

      // 3.Trigger view refresh
      queryService.execute(refreshMaterializedView(qualifiedName(definition.getViewName())));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T doPrivileged(final PrivilegedExceptionAction<T> operation)
      throws IOException {
    try {
      return AccessController.doPrivileged(operation);
    } catch (final PrivilegedActionException e) {
      throw (IOException) e.getCause();
    }
  }
}
