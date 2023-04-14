package org.opensearch.sql.datasources.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceInterfaceType;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
class KeyStoreDataSourceServiceImplTest {
  static final String NAME = "opensearch";
  static final String TEST_DS_NAME = "testDS";

  @Mock
  private DataSourceFactory dataSourceFactory;

  @Mock
  private StorageEngine storageEngine;

  private KeyStoreDataSourceServiceImpl keyStoreDataSourceService;

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
    keyStoreDataSourceService =
        new KeyStoreDataSourceServiceImpl(
            new HashSet<>() {
              {
                add(dataSourceFactory);
              }
            });
  }

  @AfterEach
  public void clear() {
    keyStoreDataSourceService.clear();
  }

  @Test
  void getDataSourceSuccess() {
    keyStoreDataSourceService.createDataSource(
        DataSourceMetadata.defaultOpenSearchDataSourceMetadata());

    Assertions.assertEquals(
        new DataSource(DEFAULT_DATASOURCE_NAME, DataSourceType.OPENSEARCH, storageEngine),
        keyStoreDataSourceService.getDataSource(DEFAULT_DATASOURCE_NAME));
  }

  @Test
  void getNotExistDataSourceShouldFail() {
    DataSourceNotFoundException exception =
        Assertions.assertThrows(DataSourceNotFoundException.class,
            () -> keyStoreDataSourceService.getDataSource("mock"));
    Assertions.assertEquals("DataSource with name mock doesn't exist.", exception.getMessage());
  }

  @Test
  void getAddDataSourcesShouldSuccess() {
    Assertions.assertEquals(0, keyStoreDataSourceService.getDataSourceMetadata(false).size());

    keyStoreDataSourceService.createDataSource(
        metadata(NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    Assertions.assertEquals(1, keyStoreDataSourceService.getDataSourceMetadata(false).size());
  }

  @Test
  void noDataSourceExistAfterClear() {
    keyStoreDataSourceService.createDataSource(
        metadata(NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    Assertions.assertEquals(1, keyStoreDataSourceService.getDataSourceMetadata(false).size());

    keyStoreDataSourceService.clear();
    Assertions.assertEquals(0, keyStoreDataSourceService.getDataSourceMetadata(false).size());
  }

  @Test
  void metaDataMissingNameShouldFail() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                keyStoreDataSourceService.createDataSource(
                    metadata(null, DataSourceType.OPENSEARCH, ImmutableMap.of())));
    Assertions.assertEquals(
        "Missing Name Field from a DataSource. Name is a required parameter.",
        exception.getMessage());
  }

  @Test
  void metaDataHasIllegalDataSourceNameShouldFail() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                keyStoreDataSourceService.createDataSource(
                    metadata("prometheus.test", DataSourceType.OPENSEARCH, ImmutableMap.of())));
    Assertions.assertEquals(
        "DataSource Name: prometheus.test contains illegal characters. "
            + "Allowed characters: a-zA-Z0-9_-*@.",
        exception.getMessage());
  }

  @Test
  void metaDataMissingPropertiesShouldFail() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> keyStoreDataSourceService.createDataSource(
                metadata(NAME, DataSourceType.OPENSEARCH, null)));
    Assertions.assertEquals(
        "Missing properties field in catalog configuration. Properties are required parameters.",
        exception.getMessage());
  }

  @Test
  void metaDataHasDuplicateNameShouldFail() {
    keyStoreDataSourceService.createDataSource(
        metadata(NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    Assertions.assertEquals(1, keyStoreDataSourceService.getDataSourceMetadata(false).size());

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> keyStoreDataSourceService.createDataSource(
                metadata(NAME, DataSourceType.OPENSEARCH, null)));
    Assertions.assertEquals(
        String.format("Datasource name should be unique, Duplicate datasource found %s.", NAME),
        exception.getMessage());
  }

  @Test
  void testDatastoreInterfaceType() {
    Assertions.assertEquals(DataSourceInterfaceType.KEYSTORE,
        this.keyStoreDataSourceService.datasourceInterfaceType());
  }

  @Test
  void testDataSourceExists() {
    Assertions.assertFalse(keyStoreDataSourceService.dataSourceExists(TEST_DS_NAME));
    keyStoreDataSourceService.createDataSource(
        metadata(TEST_DS_NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    Assertions.assertTrue(keyStoreDataSourceService.dataSourceExists(TEST_DS_NAME));
    keyStoreDataSourceService.deleteDataSource(TEST_DS_NAME);
    Assertions.assertFalse(keyStoreDataSourceService.dataSourceExists(TEST_DS_NAME));
  }

  @Test
  void testGetDataSourceMetadataByName() {
    keyStoreDataSourceService.createDataSource(
        metadata(TEST_DS_NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    DataSourceMetadata dataSourceMetadata
        = keyStoreDataSourceService.getDataSourceMetadata(TEST_DS_NAME);
    Assertions.assertEquals(TEST_DS_NAME, dataSourceMetadata.getName());
    Assertions.assertEquals(DataSourceType.OPENSEARCH, dataSourceMetadata.getConnector());
    Assertions.assertEquals(ImmutableMap.of(), dataSourceMetadata.getProperties());
  }

  @Test
  void testUpdateDataSource() {
    UnsupportedOperationException unsupportedOperationException
        = Assertions.assertThrows(UnsupportedOperationException.class,
            () -> keyStoreDataSourceService
                .updateDataSource(metadata(TEST_DS_NAME, DataSourceType.OPENSEARCH,
                    ImmutableMap.of())));
    Assertions.assertEquals("Update operation is not supported in KeyStoreDataService",
        unsupportedOperationException.getMessage());
  }

  @Test
  void testDeleteDataSource() {
    keyStoreDataSourceService.createDataSource(
        metadata(TEST_DS_NAME, DataSourceType.OPENSEARCH, ImmutableMap.of()));
    Assertions.assertTrue(keyStoreDataSourceService.dataSourceExists(TEST_DS_NAME));
    keyStoreDataSourceService.deleteDataSource(TEST_DS_NAME);
    Assertions.assertFalse(keyStoreDataSourceService.dataSourceExists(TEST_DS_NAME));
    IllegalArgumentException illegalArgumentException
        = Assertions.assertThrows(IllegalArgumentException.class,
              () ->  keyStoreDataSourceService.getDataSourceMetadata(TEST_DS_NAME));
    Assertions.assertEquals("DataSourceMetadata with name: testDS doesn't exist.",
        illegalArgumentException.getMessage());
  }

  DataSourceMetadata metadata(String name, DataSourceType type, Map<String, String> properties) {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName(name);
    dataSourceMetadata.setConnector(type);
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }
}
