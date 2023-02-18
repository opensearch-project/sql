/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

@ExtendWith(MockitoExtension.class)
public class JDBCStorageFactoryTest {
  @Mock
  private DataSourceMetadata metadata;

  @Test
  public void getDataSourceType() {
    assertEquals(DataSourceType.JDBC, new JDBCStorageFactory().getDataSourceType());
  }

  @Test
  public void createDataSource() {
    when(metadata.getName()).thenReturn("mysource");
    DataSource dataSource = new JDBCStorageFactory().createDataSource(metadata);

    assertEquals("mysource", dataSource.getName());
    assertEquals(DataSourceType.JDBC, dataSource.getConnectorType());
  }
}
