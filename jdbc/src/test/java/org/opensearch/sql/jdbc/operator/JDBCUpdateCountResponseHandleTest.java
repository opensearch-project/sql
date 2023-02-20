/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine;

class JDBCUpdateCountResponseHandleTest {

  private JDBCUpdateCountResponseHandle jdbcResponseHandle;

  @BeforeEach
  public void setup() {
    jdbcResponseHandle = new JDBCUpdateCountResponseHandle(0);
  }

  @Test
  public void hasNextAndNext() {
    assertTrue(jdbcResponseHandle.hasNext());
    assertEquals(integerValue(0), jdbcResponseHandle.next());
    assertFalse(jdbcResponseHandle.hasNext());
  }

  @Test
  public void close() {
    jdbcResponseHandle.close();
  }

  @Test
  public void schema() {
    ExecutionEngine.Schema schema = jdbcResponseHandle.schema();
    assertEquals(1, schema.getColumns().size());
    assertEquals(
        new ExecutionEngine.Schema.Column("result", "result", INTEGER), schema.getColumns().get(0));
  }
}
