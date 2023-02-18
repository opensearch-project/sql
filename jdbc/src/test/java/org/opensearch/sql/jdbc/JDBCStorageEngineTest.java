/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.exception.SemanticCheckException;

@ExtendWith(MockitoExtension.class)
class JDBCStorageEngineTest {
  @Mock private DataSourceSchemaName dataSourceSchemaName;

  @Mock private DataSourceMetadata metadata;

  @Test
  public void getTableThrowException() {
    assertThrows(
        SemanticCheckException.class,
        () -> new JDBCStorageEngine(metadata).getTable(dataSourceSchemaName, "table"));
  }

  @Test
  public void getFunctions() {
    assertEquals(1, new JDBCStorageEngine(metadata).getFunctions().size());
  }
}
