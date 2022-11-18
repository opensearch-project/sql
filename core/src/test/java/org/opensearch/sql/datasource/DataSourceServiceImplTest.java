/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
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
class DataSourceServiceImplTest {

  static final String NAME = "opensearch";

  @Mock private DataSourceFactory dataSourceFactory;

  @Mock private StorageEngine storageEngine;

  private DataSourceService dataSourceService;

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
    dataSourceService =
        new DataSourceServiceImpl(
            new HashSet<>() {
              {
                add(dataSourceFactory);
              }
            });
  }

  @AfterEach
  public void clear() {
    dataSourceService.clear();
  }

  @Test
  void getDataSourceSuccess() {
    dataSourceService.addDataSource(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());

    assertEquals(
        new DataSource(DEFAULT_DATASOURCE_NAME, DataSourceType.OPENSEARCH, storageEngine),
        dataSourceService.getDataSource(DEFAULT_DATASOURCE_NAME));
  }

  @Test
  void getNotExistDataSourceShouldFail() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> dataSourceService.getDataSource("mock"));
    assertEquals("DataSource with name mock doesn't exist.", exception.getMessage());
  }

  @Test
  void getAddDataSourcesShouldSuccess() {
    assertEquals(0, dataSourceService.getDataSources().size());

    dataSourceService.addDataSource(metadata(NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    assertEquals(1, dataSourceService.getDataSources().size());
  }

  @Test
  void noDataSourceExistAfterClear() {
    dataSourceService.addDataSource(metadata(NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    assertEquals(1, dataSourceService.getDataSources().size());

    dataSourceService.clear();
    assertEquals(0, dataSourceService.getDataSources().size());
  }

  @Test
  void metaDataMissingNameShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                dataSourceService.addDataSource(
                    metadata(null, DataSourceType.OPENSEARCH, ImmutableMap.of())));
    assertEquals(
        "Missing Name Field from a DataSource. Name is a required parameter.",
        exception.getMessage());
  }

  @Test
  void metaDataHasIllegalDataSourceNameShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                dataSourceService.addDataSource(
                    metadata("prometheus.test", DataSourceType.OPENSEARCH, ImmutableMap.of())));
    assertEquals(
        "DataSource Name: prometheus.test contains illegal characters. "
            + "Allowed characters: a-zA-Z0-9_-*@.",
        exception.getMessage());
  }

  @Test
  void metaDataMissingPropertiesShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dataSourceService.addDataSource(metadata(NAME, DataSourceType.OPENSEARCH, null)));
    assertEquals(
        "Missing properties field in catalog configuration. Properties are required parameters.",
        exception.getMessage());
  }

  @Test
  void metaDataHasDuplicateNameShouldFail() {
    dataSourceService.addDataSource(metadata(NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    assertEquals(1, dataSourceService.getDataSources().size());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dataSourceService.addDataSource(metadata(NAME, DataSourceType.OPENSEARCH, null)));
    assertEquals(
        String.format("Datasource name should be unique, Duplicate datasource found %s.", NAME),
        exception.getMessage());
  }

  DataSourceMetadata metadata(String name, DataSourceType type, Map<String, String> properties) {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName(name);
    dataSourceMetadata.setConnector(type);
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }
}
