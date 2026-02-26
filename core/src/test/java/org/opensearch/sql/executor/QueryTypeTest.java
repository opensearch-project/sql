/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class QueryTypeTest {

  @Test
  void clickhouse_enum_value_exists() {
    // Verify CLICKHOUSE is a valid QueryType value
    QueryType clickhouse = QueryType.CLICKHOUSE;
    assertTrue(clickhouse.isDialectQuery());
  }

  @Test
  void ppl_is_not_dialect_query() {
    assertFalse(QueryType.PPL.isDialectQuery());
  }

  @Test
  void sql_is_not_dialect_query() {
    assertFalse(QueryType.SQL.isDialectQuery());
  }

  @Test
  void clickhouse_is_dialect_query() {
    assertTrue(QueryType.CLICKHOUSE.isDialectQuery());
  }
}
