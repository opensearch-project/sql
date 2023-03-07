/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
class DataSourceServiceImplTest {

  static final String NAME = "opensearch";

  @Mock
  private DataSourceFactory dataSourceFactory;
  @Mock
  private StorageEngine storageEngine;
  @Mock
  private DataSourceMetadataStorage dataSourceMetadataStorage;

  @Mock
  private DataSourceUserAuthorizationHelper dataSourceUserAuthorizationHelper;

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
            }, dataSourceMetadataStorage,
            dataSourceUserAuthorizationHelper);
  }

  @Test
  void testGetDataSourceForDefaultOpenSearchDataSource() {
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(any());
    dataSourceService.createDataSource(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());
    assertEquals(
        new DataSource(DEFAULT_DATASOURCE_NAME, DataSourceType.OPENSEARCH, storageEngine),
        dataSourceService.getDataSource(DEFAULT_DATASOURCE_NAME));
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  @Test
  void testGetDataSourceForNonExistingDataSource() {
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(Optional.empty());
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                dataSourceService.getDataSource("test"));
    assertEquals("DataSource with name test doesn't exist.", exception.getMessage());
    verify(dataSourceMetadataStorage, times(1))
        .getDataSourceMetadata("test");
  }

  @Test
  void testGetDataSourceSuccessCase() {
    DataSourceMetadata dataSourceMetadata = metadata("test", DataSourceType.OPENSEARCH,
        Collections.emptyList(), ImmutableMap.of());
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(Optional.of(dataSourceMetadata));
    DataSource dataSource = dataSourceService.getDataSource("test");
    assertEquals("test", dataSource.getName());
    assertEquals(DataSourceType.OPENSEARCH, dataSource.getConnectorType());
    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata("test");
    verify(dataSourceFactory, times(1))
        .createDataSource(dataSourceMetadata);
  }

  @Test
  void testGetDataSourceWithAuthorizationFailure() {
    DataSourceMetadata dataSourceMetadata = metadata("test", DataSourceType.OPENSEARCH,
        Collections.singletonList("prometheus_access"), ImmutableMap.of());
    doThrow(new SecurityException("User is not authorized to access datasource test. "
            + "User should be mapped to any of the roles in [prometheus_access] for access."))
        .when(dataSourceUserAuthorizationHelper)
        .authorizeDataSource(dataSourceMetadata);
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(Optional.of(dataSourceMetadata));


    SecurityException securityException
        = Assertions.assertThrows(SecurityException.class,
              () -> dataSourceService.getDataSource("test"));
    Assertions.assertEquals("User is not authorized to access datasource test. "
            + "User should be mapped to any of the roles in [prometheus_access] for access.",
        securityException.getMessage());

    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata("test");
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
  }


  @Test
  void testCreateDataSourceSuccessCase() {

    DataSourceMetadata dataSourceMetadata = metadata("testDS", DataSourceType.OPENSEARCH,
        Collections.emptyList(), ImmutableMap.of());
    dataSourceService.createDataSource(dataSourceMetadata);
    verify(dataSourceMetadataStorage, times(1))
        .createDataSourceMetadata(dataSourceMetadata);
    verify(dataSourceFactory, times(1))
        .createDataSource(dataSourceMetadata);

    when(dataSourceMetadataStorage.getDataSourceMetadata("testDS"))
        .thenReturn(Optional.ofNullable(metadata("testDS", DataSourceType.OPENSEARCH,
            Collections.emptyList(), ImmutableMap.of())));
    DataSource dataSource = dataSourceService.getDataSource("testDS");
    assertEquals("testDS", dataSource.getName());
    assertEquals(storageEngine, dataSource.getStorageEngine());
    assertEquals(DataSourceType.OPENSEARCH, dataSource.getConnectorType());
    verifyNoMoreInteractions(dataSourceFactory);
  }

  @Test
  void testCreateDataSourceWithDisallowedDatasourceName() {
    DataSourceMetadata dataSourceMetadata = metadata("testDS$$$", DataSourceType.OPENSEARCH,
        Collections.emptyList(), ImmutableMap.of());
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                dataSourceService.createDataSource(dataSourceMetadata));
    assertEquals("DataSource Name: testDS$$$ contains illegal characters."
            + " Allowed characters: a-zA-Z0-9_-*@.",
        exception.getMessage());
    verify(dataSourceFactory, times(1)).getDataSourceType();
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  @Test
  void testCreateDataSourceWithEmptyDatasourceName() {
    DataSourceMetadata dataSourceMetadata = metadata("", DataSourceType.OPENSEARCH,
        Collections.emptyList(), ImmutableMap.of());
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                dataSourceService.createDataSource(dataSourceMetadata));
    assertEquals("Missing Name Field from a DataSource. Name is a required parameter.",
        exception.getMessage());
    verify(dataSourceFactory, times(1)).getDataSourceType();
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  @Test
  void testCreateDataSourceWithNullParameters() {
    DataSourceMetadata dataSourceMetadata = metadata("testDS", DataSourceType.OPENSEARCH,
        Collections.emptyList(), null);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                dataSourceService.createDataSource(dataSourceMetadata));
    assertEquals("Missing properties field in datasource configuration. "
            + "Properties are required parameters.",
        exception.getMessage());
    verify(dataSourceFactory, times(1)).getDataSourceType();
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  @Test
  void testGetDataSourceMetadataSet() {
    when(dataSourceMetadataStorage.getDataSourceMetadata()).thenReturn(new ArrayList<>() {
      {
        add(metadata("testDS", DataSourceType.PROMETHEUS, Collections.emptyList(),
            ImmutableMap.of()));
      }
    });
    Set<DataSourceMetadata> dataSourceMetadataSet
        = dataSourceService.getDataSourceMetadataSet();
    assertEquals(2, dataSourceMetadataSet.size());
    assertTrue(dataSourceMetadataSet
        .contains(DataSourceMetadata.defaultOpenSearchDataSourceMetadata()));
    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata();
  }

  @Test
  void testUpdateDatasource() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> dataSourceService.updateDataSource(new DataSourceMetadata()));
  }

  @Test
  void testDeleteDatasource() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> dataSourceService.deleteDataSource(NAME));
  }

  @Test
  void testDataSourceExists() {
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(Optional.empty());
    Assertions.assertFalse(dataSourceService.dataSourceExists("test"));
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(Optional.of(metadata("test", DataSourceType.PROMETHEUS,
            List.of(), ImmutableMap.of())));
    Assertions.assertTrue(dataSourceService.dataSourceExists("test"));
  }

  @Test
  void testDataSourceExistsForDefaultDataSource() {
    Assertions.assertTrue(dataSourceService.dataSourceExists(DEFAULT_DATASOURCE_NAME));
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  DataSourceMetadata metadata(String name, DataSourceType type,
                              List<String> allowedRoles,
                              Map<String, String> properties) {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName(name);
    dataSourceMetadata.setConnector(type);
    dataSourceMetadata.setAllowedRoles(allowedRoles);
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }
}
