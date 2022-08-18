/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl;

import org.opensearch.sql.storage.StorageEngine;

/**
 * Data definition task interface.
 */
public abstract class DataDefinitionTask {

  protected final QueryService queryService;

  protected final StorageEngine systemCatalog;

  protected DataDefinitionTask(QueryService queryService,
                              StorageEngine systemCatalog) {
    this.queryService = queryService;
    this.systemCatalog = systemCatalog;
  }

  /**
   * Execute DDL statement.
   */
  public abstract void execute();

}