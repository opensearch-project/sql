/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

public enum QueryType {
  PPL,
  SQL,
  CLICKHOUSE;

  /** Returns true if this query type represents a third-party dialect. */
  public boolean isDialectQuery() {
    return this != PPL && this != SQL;
  }
}
