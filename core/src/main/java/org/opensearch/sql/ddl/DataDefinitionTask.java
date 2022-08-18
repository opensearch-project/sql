/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl;

/**
 * Data definition task interface.
 */
public interface DataDefinitionTask {

  /**
   * Execute DDL against the given system storage and query statement (if applicable).
   */
  void execute();

}
