/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl;

import lombok.Setter;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Data definition task interface.
 */
@Setter
public abstract class DataDefinitionTask {

  protected QueryService queryService;

  protected StorageEngine systemCatalog;

  /**
   * Execute DDL statement.
   */
  public abstract void execute();

}