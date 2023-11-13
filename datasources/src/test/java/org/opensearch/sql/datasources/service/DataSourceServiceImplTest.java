/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelper;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
class DataSourceServiceImplTest {

  @Mock private DataSourceFactory dataSourceFactory;
  @Mock private StorageEngine storageEngine;
  @Mock private DataSourceMetadataStorage dataSourceMetadataStorage;

  @Mock private DataSourceUserAuthorizationHelper dataSourceUserAuthorizationHelper;

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
            },
            dataSourceMetadataStorage,
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
    when(dataSourceMetadataStorage.getDataSourceMetadata("test")).thenReturn(Optional.empty());
    DataSourceNotFoundException exception =
        assertThrows(
            DataSourceNotFoundException.class, () -> dataSourceService.getDataSource("test"));
    assertEquals("DataSource with name test doesn't exist.", exception.getMessage());
    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata("test");
  }

  @Test
  void testGetDataSourceSuccessCase() {
    DataSourceMetadata dataSourceMetadata =
        metadata("test", DataSourceType.OPENSEARCH, Collections.emptyList(), ImmutableMap.of());
    doNothing().when(dataSourceUserAuthorizationHelper).authorizeDataSource(dataSourceMetadata);
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(Optional.of(dataSourceMetadata));
    DataSource dataSource = dataSourceService.getDataSource("test");
    assertEquals("test", dataSource.getName());
    assertEquals(DataSourceType.OPENSEARCH, dataSource.getConnectorType());
    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata("test");
    verify(dataSourceFactory, times(1)).createDataSource(dataSourceMetadata);
  }

  @Test
  void testGetDataSourceWithAuthorizationFailure() {
    DataSourceMetadata dataSourceMetadata =
        metadata(
            "test",
            DataSourceType.OPENSEARCH,
            Collections.singletonList("prometheus_access"),
            ImmutableMap.of());
    doThrow(
            new SecurityException(
                "User is not authorized to access datasource test. User should be mapped to any of"
                    + " the roles in [prometheus_access] for access."))
        .when(dataSourceUserAuthorizationHelper)
        .authorizeDataSource(dataSourceMetadata);
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(Optional.of(dataSourceMetadata));

    SecurityException securityException =
        Assertions.assertThrows(
            SecurityException.class, () -> dataSourceService.getDataSource("test"));
    Assertions.assertEquals(
        "User is not authorized to access datasource test. "
            + "User should be mapped to any of the roles in [prometheus_access] for access.",
        securityException.getMessage());

    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata("test");
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
  }

  @Test
  void testCreateDataSourceSuccessCase() {

    DataSourceMetadata dataSourceMetadata =
        metadata("testDS", DataSourceType.OPENSEARCH, Collections.emptyList(), ImmutableMap.of());
    dataSourceService.createDataSource(dataSourceMetadata);
    verify(dataSourceMetadataStorage, times(1)).createDataSourceMetadata(dataSourceMetadata);
    verify(dataSourceFactory, times(1)).createDataSource(dataSourceMetadata);

    when(dataSourceMetadataStorage.getDataSourceMetadata("testDS"))
        .thenReturn(
            Optional.ofNullable(
                metadata(
                    "testDS",
                    DataSourceType.OPENSEARCH,
                    Collections.emptyList(),
                    ImmutableMap.of())));
    DataSource dataSource = dataSourceService.getDataSource("testDS");
    assertEquals("testDS", dataSource.getName());
    assertEquals(storageEngine, dataSource.getStorageEngine());
    assertEquals(DataSourceType.OPENSEARCH, dataSource.getConnectorType());
  }

  @Test
  void testCreateDataSourceWithDisallowedDatasourceName() {
    DataSourceMetadata dataSourceMetadata =
        metadata(
            "testDS$$$", DataSourceType.OPENSEARCH, Collections.emptyList(), ImmutableMap.of());
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dataSourceService.createDataSource(dataSourceMetadata));
    assertEquals(
        "DataSource Name: testDS$$$ contains illegal characters."
            + " Allowed characters: a-zA-Z0-9_-*@.",
        exception.getMessage());
    verify(dataSourceFactory, times(1)).getDataSourceType();
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  @Test
  void testCreateDataSourceWithEmptyDatasourceName() {
    DataSourceMetadata dataSourceMetadata =
        metadata("", DataSourceType.OPENSEARCH, Collections.emptyList(), ImmutableMap.of());
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dataSourceService.createDataSource(dataSourceMetadata));
    assertEquals(
        "Missing Name Field from a DataSource. Name is a required parameter.",
        exception.getMessage());
    verify(dataSourceFactory, times(1)).getDataSourceType();
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  @Test
  void testCreateDataSourceWithNullParameters() {
    DataSourceMetadata dataSourceMetadata =
        metadata("testDS", DataSourceType.OPENSEARCH, Collections.emptyList(), null);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dataSourceService.createDataSource(dataSourceMetadata));
    assertEquals(
        "Missing properties field in datasource configuration. "
            + "Properties are required parameters.",
        exception.getMessage());
    verify(dataSourceFactory, times(1)).getDataSourceType();
    verify(dataSourceFactory, times(0)).createDataSource(dataSourceMetadata);
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  @Test
  void testGetDataSourceMetadataSet() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://localhost:9200");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "username");
    properties.put("prometheus.auth.password", "password");
    when(dataSourceMetadataStorage.getDataSourceMetadata())
        .thenReturn(
            new ArrayList<>() {
              {
                add(
                    metadata(
                        "testDS", DataSourceType.PROMETHEUS, Collections.emptyList(), properties));
              }
            });
    Set<DataSourceMetadata> dataSourceMetadataSet = dataSourceService.getDataSourceMetadata(false);
    assertEquals(1, dataSourceMetadataSet.size());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataSet.iterator().next();
    assertTrue(dataSourceMetadata.getProperties().containsKey("prometheus.uri"));
    assertTrue(dataSourceMetadata.getProperties().containsKey("prometheus.auth.type"));
    assertFalse(dataSourceMetadata.getProperties().containsKey("prometheus.auth.username"));
    assertFalse(dataSourceMetadata.getProperties().containsKey("prometheus.auth.password"));
    assertFalse(
        dataSourceMetadataSet.contains(DataSourceMetadata.defaultOpenSearchDataSourceMetadata()));
    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata();
  }

  @Test
  void testGetDataSourceMetadataSetWithDefaultDatasource() {
    when(dataSourceMetadataStorage.getDataSourceMetadata())
        .thenReturn(
            new ArrayList<>() {
              {
                add(
                    metadata(
                        "testDS",
                        DataSourceType.PROMETHEUS,
                        Collections.emptyList(),
                        ImmutableMap.of()));
              }
            });
    Set<DataSourceMetadata> dataSourceMetadataSet = dataSourceService.getDataSourceMetadata(true);
    assertEquals(2, dataSourceMetadataSet.size());
    assertTrue(
        dataSourceMetadataSet.contains(DataSourceMetadata.defaultOpenSearchDataSourceMetadata()));
    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata();
  }

  @Test
  void testUpdateDataSourceSuccessCase() {

    DataSourceMetadata dataSourceMetadata =
        metadata("testDS", DataSourceType.OPENSEARCH, Collections.emptyList(), ImmutableMap.of());
    dataSourceService.updateDataSource(dataSourceMetadata);
    verify(dataSourceMetadataStorage, times(1)).updateDataSourceMetadata(dataSourceMetadata);
    verify(dataSourceFactory, times(1)).createDataSource(dataSourceMetadata);
  }

  @Test
  void testUpdateDefaultDataSource() {
    DataSourceMetadata dataSourceMetadata =
        metadata(
            DEFAULT_DATASOURCE_NAME,
            DataSourceType.OPENSEARCH,
            Collections.emptyList(),
            ImmutableMap.of());
    UnsupportedOperationException unsupportedOperationException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> dataSourceService.updateDataSource(dataSourceMetadata));
    assertEquals(
        "Not allowed to update default datasource :" + DEFAULT_DATASOURCE_NAME,
        unsupportedOperationException.getMessage());
  }

  @Test
  void testDeleteDatasource() {
    dataSourceService.deleteDataSource("testDS");
    verify(dataSourceMetadataStorage, times(1)).deleteDataSourceMetadata("testDS");
  }

  @Test
  void testDeleteDefaultDatasource() {
    UnsupportedOperationException unsupportedOperationException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> dataSourceService.deleteDataSource(DEFAULT_DATASOURCE_NAME));
    assertEquals(
        "Not allowed to delete default datasource :" + DEFAULT_DATASOURCE_NAME,
        unsupportedOperationException.getMessage());
  }

  @Test
  void testDataSourceExists() {
    when(dataSourceMetadataStorage.getDataSourceMetadata("test")).thenReturn(Optional.empty());
    Assertions.assertFalse(dataSourceService.dataSourceExists("test"));
    when(dataSourceMetadataStorage.getDataSourceMetadata("test"))
        .thenReturn(
            Optional.of(metadata("test", DataSourceType.PROMETHEUS, List.of(), ImmutableMap.of())));
    Assertions.assertTrue(dataSourceService.dataSourceExists("test"));
  }

  @Test
  void testDataSourceExistsForDefaultDataSource() {
    Assertions.assertTrue(dataSourceService.dataSourceExists(DEFAULT_DATASOURCE_NAME));
    verifyNoInteractions(dataSourceMetadataStorage);
  }

  DataSourceMetadata metadata(
      String name, DataSourceType type, List<String> allowedRoles, Map<String, String> properties) {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName(name);
    dataSourceMetadata.setConnector(type);
    dataSourceMetadata.setAllowedRoles(allowedRoles);
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }

  @Test
  void testRemovalOfAuthorizationInfo() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "https://localhost:9090");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "username");
    properties.put("prometheus.auth.password", "password");
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata(
            "testDS",
            DataSourceType.PROMETHEUS,
            Collections.singletonList("prometheus_access"),
            properties,
            null);
    when(dataSourceMetadataStorage.getDataSourceMetadata("testDS"))
        .thenReturn(Optional.of(dataSourceMetadata));

    DataSourceMetadata dataSourceMetadata1 = dataSourceService.getDataSourceMetadata("testDS");
    assertEquals("testDS", dataSourceMetadata1.getName());
    assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata1.getConnector());
    assertTrue(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.type"));
    assertFalse(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.username"));
    assertFalse(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.password"));
  }

  @Test
  void testRemovalOfAuthorizationInfoForAccessKeyAndSecretKye() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "https://localhost:9090");
    properties.put("prometheus.auth.type", "awssigv4");
    properties.put("prometheus.auth.access_key", "access_key");
    properties.put("prometheus.auth.secret_key", "secret_key");
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata(
            "testDS",
            DataSourceType.PROMETHEUS,
            Collections.singletonList("prometheus_access"),
            properties,
            null);
    when(dataSourceMetadataStorage.getDataSourceMetadata("testDS"))
        .thenReturn(Optional.of(dataSourceMetadata));

    DataSourceMetadata dataSourceMetadata1 = dataSourceService.getDataSourceMetadata("testDS");
    assertEquals("testDS", dataSourceMetadata1.getName());
    assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata1.getConnector());
    assertTrue(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.type"));
    assertFalse(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.access_key"));
    assertFalse(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.secret_key"));
  }

  @Test
  void testRemovalOfAuthorizationInfoForGlueWithRoleARN() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "basicauth");
    properties.put("glue.indexstore.opensearch.auth.username", "username");
    properties.put("glue.indexstore.opensearch.auth.password", "password");
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata(
            "testGlue",
            DataSourceType.S3GLUE,
            Collections.singletonList("glue_access"),
            properties,
            null);
    when(dataSourceMetadataStorage.getDataSourceMetadata("testGlue"))
        .thenReturn(Optional.of(dataSourceMetadata));

    DataSourceMetadata dataSourceMetadata1 = dataSourceService.getDataSourceMetadata("testGlue");
    assertEquals("testGlue", dataSourceMetadata1.getName());
    assertEquals(DataSourceType.S3GLUE, dataSourceMetadata1.getConnector());
    assertTrue(dataSourceMetadata1.getProperties().containsKey("glue.auth.type"));
    assertTrue(dataSourceMetadata1.getProperties().containsKey("glue.auth.role_arn"));
    assertTrue(dataSourceMetadata1.getProperties().containsKey("glue.indexstore.opensearch.uri"));
    assertTrue(dataSourceMetadata1.getProperties().containsKey("glue.indexstore.opensearch.auth"));
    assertFalse(
        dataSourceMetadata1
            .getProperties()
            .containsKey("glue.indexstore.opensearch.auth.username"));
    assertFalse(
        dataSourceMetadata1
            .getProperties()
            .containsKey("glue.indexstore.opensearch.auth.password"));
  }

  @Test
  void testGetDataSourceMetadataForNonExistingDataSource() {
    when(dataSourceMetadataStorage.getDataSourceMetadata("testDS")).thenReturn(Optional.empty());
    DataSourceNotFoundException exception =
        assertThrows(
            DataSourceNotFoundException.class,
            () -> dataSourceService.getDataSourceMetadata("testDS"));
    assertEquals("DataSource with name testDS doesn't exist.", exception.getMessage());
  }

  @Test
  void testGetDataSourceMetadataForSpecificDataSourceName() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://localhost:9200");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "username");
    properties.put("prometheus.auth.password", "password");
    when(dataSourceMetadataStorage.getDataSourceMetadata("testDS"))
        .thenReturn(
            Optional.ofNullable(
                metadata(
                    "testDS", DataSourceType.PROMETHEUS, Collections.emptyList(), properties)));
    DataSourceMetadata dataSourceMetadata = this.dataSourceService.getDataSourceMetadata("testDS");
    assertTrue(dataSourceMetadata.getProperties().containsKey("prometheus.uri"));
    assertTrue(dataSourceMetadata.getProperties().containsKey("prometheus.auth.type"));
    assertFalse(dataSourceMetadata.getProperties().containsKey("prometheus.auth.username"));
    assertFalse(dataSourceMetadata.getProperties().containsKey("prometheus.auth.password"));
    verify(dataSourceMetadataStorage, times(1)).getDataSourceMetadata("testDS");
  }

  @Test
  void testGetRawDataSourceMetadata() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "https://localhost:9090");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "username");
    properties.put("prometheus.auth.password", "password");
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata(
            "testDS",
            DataSourceType.PROMETHEUS,
            Collections.singletonList("prometheus_access"),
            properties,
            null);
    when(dataSourceMetadataStorage.getDataSourceMetadata("testDS"))
        .thenReturn(Optional.of(dataSourceMetadata));

    DataSourceMetadata dataSourceMetadata1 = dataSourceService.getRawDataSourceMetadata("testDS");
    assertEquals("testDS", dataSourceMetadata1.getName());
    assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata1.getConnector());
    assertTrue(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.type"));
    assertTrue(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.username"));
    assertTrue(dataSourceMetadata1.getProperties().containsKey("prometheus.auth.password"));
  }
}
