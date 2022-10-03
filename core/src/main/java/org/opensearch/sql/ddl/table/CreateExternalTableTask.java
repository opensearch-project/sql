/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.table;

import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.ast.dsl.AstDSL.write;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ddl.Column;
import org.opensearch.sql.ddl.DataDefinitionTask;

/**
 * Create external table task.
 */
@RequiredArgsConstructor
public class CreateExternalTableTask extends DataDefinitionTask {

  private final QualifiedName tableName;

  private final List<Column> columns;

  private final String fileFormat;

  private final String location;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void execute() {
    try {
      Map<String, String> colNameTypes = columns.stream()
          .collect(Collectors.toMap(Column::getName, Column::getType));

      UnresolvedPlan insertSysTable = write(
          values(
              Arrays.asList(
                  stringLiteral(tableName.toString()),
                  stringLiteral(OBJECT_MAPPER.writeValueAsString(colNameTypes)),
                  stringLiteral(fileFormat),
                  stringLiteral(location))),
          qualifiedName(".extables"),
          Arrays.asList(
              qualifiedName("tableName"),
              qualifiedName("columns"),
              qualifiedName("fileFormat"),
              qualifiedName("location")));

      queryService.execute(insertSysTable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
