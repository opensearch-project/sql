/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
class DataSourceLoaderCacheImplTest {

  @Mock
  private DataSourceFactory dataSourceFactory;

  @Mock
  private StorageEngine storageEngine;

  @BeforeEach
  public void setup() {
    lenient()
        .doAnswer(
            invocation -> {
              DataSourceMetadata metadata = invocation.getArgument(0);
              return new DataSource(metadata.getName(), metadata.getConnector(), storageEngine);
            })
        .when(dataSourceFactory)
        .createDataSource(any());
    when(dataSourceFactory.getDataSourceType()).thenReturn(DataSourceType.OPENSEARCH);
  }

  @Test
  void testGetOrLoadDataSource() {
    DataSourceLoaderCache dataSourceLoaderCache =
        new DataSourceLoaderCacheImpl(Collections.singleton(dataSourceFactory));
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.OPENSEARCH);
    dataSourceMetadata.setAllowedRoles(Collections.emptyList());
    dataSourceMetadata.setProperties(ImmutableMap.of());
    DataSource dataSource = dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
    verify(dataSourceFactory, times(1)).createDataSource(dataSourceMetadata);
    Assertions.assertEquals(dataSource,
        dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata));
    verifyNoMoreInteractions(dataSourceFactory);
  }

  @Test
  void testGetOrLoadDataSourceWithMetadataUpdate() {
    DataSourceLoaderCache dataSourceLoaderCache =
        new DataSourceLoaderCacheImpl(Collections.singleton(dataSourceFactory));
    DataSourceMetadata dataSourceMetadata = getMetadata();
    dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
    dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
    dataSourceMetadata.setAllowedRoles(List.of("testDS_access"));
    dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
    dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
    verify(dataSourceFactory, times(2)).createDataSource(dataSourceMetadata);
  }

  private DataSourceMetadata getMetadata() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.OPENSEARCH);
    dataSourceMetadata.setAllowedRoles(Collections.emptyList());
    dataSourceMetadata.setProperties(ImmutableMap.of());
    return dataSourceMetadata;
  }

}
