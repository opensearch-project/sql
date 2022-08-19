/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.table;

import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.ddl.QueryService;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Create table task.
 */
public class CreateTableTask extends DataDefinitionTask {

  private final QualifiedName tableName;

  //private final

  public CreateTableTask(QualifiedName tableName) {
    this.tableName = tableName;
  }

  @Override
  public void execute() {
  }
}
