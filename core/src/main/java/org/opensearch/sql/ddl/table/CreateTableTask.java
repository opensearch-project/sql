/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.table;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.ddl.Column;
import org.opensearch.sql.ddl.DataDefinitionTask;

/**
 * Create table task.
 */
@RequiredArgsConstructor
public class CreateTableTask extends DataDefinitionTask {

  private final QualifiedName tableName;

  private final List<Column> columns;

  @Override
  public ExprValue execute() {
    // 1.Check if any conflicting view/table

    // 2.Create table
    systemCatalog.addTable(tableName.toString(), columns);

    return ExprValueUtils.missingValue();
  }
}
