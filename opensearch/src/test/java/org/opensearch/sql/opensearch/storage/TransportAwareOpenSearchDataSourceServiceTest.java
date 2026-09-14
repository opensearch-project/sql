/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
class TransportAwareOpenSearchDataSourceServiceTest {
  @Mock private DataSourceService delegate;
  @Mock private OpenSearchClient client;
  @Mock private Settings settings;
  @Mock private StorageEngine storageEngine;

  private TransportAwareOpenSearchDataSourceService service;

  @BeforeEach
  void setUp() {
    service = new TransportAwareOpenSearchDataSourceService(delegate, client, settings);
  }

  @Test
  void replacesOpenSearchStorageEngineAfterDelegatedAuthorization() {
    DataSource original = new DataSource("@opensearch", DataSourceType.OPENSEARCH, storageEngine);
    when(delegate.getDataSource("@opensearch")).thenReturn(original);

    DataSource actual = service.getDataSource("@opensearch");

    verify(delegate).getDataSource("@opensearch");
    assertEquals(original.getName(), actual.getName());
    assertSame(DataSourceType.OPENSEARCH, actual.getConnectorType());
    assertTrue(actual.getStorageEngine() instanceof OpenSearchStorageEngine);
  }

  @Test
  void preservesNonOpenSearchDataSource() {
    DataSource original = new DataSource("prometheus", DataSourceType.PROMETHEUS, storageEngine);
    when(delegate.getDataSource("prometheus")).thenReturn(original);

    assertSame(original, service.getDataSource("prometheus"));
  }
}
