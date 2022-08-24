/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import static org.opensearch.sql.ast.dsl.AstDSL.deleteAll;
import static org.opensearch.sql.ast.dsl.AstDSL.equalTo;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.write;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Refresh materialized view task.
 */
@RequiredArgsConstructor
@ToString
public class RefreshMaterializedViewTask extends DataDefinitionTask {

  private final QualifiedName viewName;

  @Override
  public void execute() {
    try {
      // Read from system table to find refresh query
      ExecutionEngine.QueryResponse resp = queryService.execute(
          project(
              filter(
                  relation(".matviews"),
                  equalTo(field("viewName"), stringLiteral(viewName.toString()))
              ),
              field("query"), field("columns")));

//      queryService.execute(deleteAll(viewName));

      ExprTupleValue value = (ExprTupleValue) resp.getResults().get(0);
      ExprValue queryString = value.tupleValue().get("query");
      ExprValue columnsString = value.tupleValue().get("columns");

      NodeSerializer serializer = new NodeSerializer();
      ObjectMapper OBJECT_MAPPER = new ObjectMapper();
//      UnresolvedPlan query = OBJECT_MAPPER.readValue(queryString.stringValue(),
//          Project.class);

      UnresolvedPlan query = (UnresolvedPlan) serializer.deserializeNode(queryString.stringValue());
      List<String> columns = OBJECT_MAPPER.readValue(columnsString.stringValue(),
          List.class);
      queryService.execute(write(query, viewName,
          columns.stream().map(QualifiedName::new).collect(Collectors.toList())));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }
}
