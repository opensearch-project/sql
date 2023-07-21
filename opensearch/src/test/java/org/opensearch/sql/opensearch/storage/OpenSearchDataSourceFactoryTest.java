/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@ExtendWith(MockitoExtension.class)
class OpenSearchDataSourceFactoryTest {

  @Mock private OpenSearchClient client;

  @Mock private Settings settings;

  @Mock private DataSourceMetadata dataSourceMetadata;

  private OpenSearchDataSourceFactory factory;

  @BeforeEach
  public void setup() {
    factory = new OpenSearchDataSourceFactory(client, settings);
  }

  @Test
  void getDataSourceType() {
    assertEquals(DataSourceType.OPENSEARCH, factory.getDataSourceType());
  }

  @Test
  void createDataSource() {
    when(dataSourceMetadata.getName()).thenReturn("opensearch");

    DataSource dataSource = factory.createDataSource(dataSourceMetadata);
    assertEquals("opensearch", dataSource.getName());
    assertEquals(DataSourceType.OPENSEARCH, dataSource.getConnectorType());
  }
}
