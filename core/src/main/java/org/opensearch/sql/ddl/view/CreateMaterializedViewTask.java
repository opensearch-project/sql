/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import static org.opensearch.sql.ast.dsl.AstDSL.createTable;
import static org.opensearch.sql.ast.dsl.AstDSL.longLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.ast.dsl.AstDSL.write;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
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
  public ExprValue execute() {
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

      // 3.Refresh state views
      UnresolvedPlan insertStateView =
          write(
              values(
                  Arrays.asList(
                      stringLiteral(definition.getViewName()),
                      stringLiteral("view is empty"),
                      longLiteral(System.currentTimeMillis()))),
              qualifiedName(".stateviews"),
              Arrays.asList(
                  qualifiedName("viewName"),
                  qualifiedName("viewstatus"),
                  qualifiedName("timestamp")));
      queryService.execute(insertStateView);
      return ExprValueUtils.missingValue();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
