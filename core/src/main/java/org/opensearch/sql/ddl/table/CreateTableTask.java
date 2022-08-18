/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.table;

import org.opensearch.sql.storage.StorageEngine;

/**
 * Create table task.
 */
public class CreateTableTask extends TableDataDefinitionTask {

  private final Schema schema;

  public CreateTableTask(StorageEngine storageEngine,
                         Schema schema) {
    super(storageEngine);
    this.schema = schema;
  }

  @Override
  public void execute() {
    storageEngine.addTable(schema);
  }
}
