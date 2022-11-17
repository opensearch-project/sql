/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.datasource.model.ConnectorType;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.storage.StorageEngine;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceServiceImplTest {

  public static final String DATASOURCE_SETTING_METADATA_KEY =
      "plugins.query.federation.datasources.config";

  @Mock
  private StorageEngine storageEngine;

  @SneakyThrows
  @Test
  public void testLoadConnectors() {
    Settings settings = getDataSourceSettings("datasources.json");
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    Set<DataSource> expected = new HashSet<>() {{
        add(new DataSource("prometheus", ConnectorType.PROMETHEUS, storageEngine));
      }};
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
  }


  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMultipleDataSources() {
    Settings settings = getDataSourceSettings("multiple_datasources.json");
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    Set<DataSource> expected = new HashSet<>() {{
        add(new DataSource("prometheus", ConnectorType.PROMETHEUS, storageEngine));
        add(new DataSource("prometheus-1", ConnectorType.PROMETHEUS, storageEngine));
      }};
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMissingName() {
    Settings settings = getDataSourceSettings("datasource_missing_name.json");
    Set<DataSource> expected = DataSourceServiceImpl.getInstance().getDataSources();
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithDuplicateDataSourceNames() {
    Settings settings = getDataSourceSettings("duplicate_datasource_names.json");
    Set<DataSource> expected = DataSourceServiceImpl.getInstance().getDataSources();
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMalformedJson() {
    Settings settings = getDataSourceSettings("malformed_datasources.json");
    Set<DataSource> expected = DataSourceServiceImpl.getInstance().getDataSources();
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
  }

  @SneakyThrows
  @Test
  public void testGetStorageEngineAfterGetDataSources() {
    Settings settings = getDataSourceSettings("empty_datasource.json");
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    DataSourceServiceImpl.getInstance().registerDefaultOpenSearchDataSource(storageEngine);
    Set<DataSource> expected = new HashSet<>();
    expected.add(new DataSource(DEFAULT_DATASOURCE_NAME, ConnectorType.OPENSEARCH, storageEngine));
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
    Assert.assertEquals(storageEngine,
        DataSourceServiceImpl.getInstance()
            .getDataSource(DEFAULT_DATASOURCE_NAME).getStorageEngine());
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
    Assert.assertEquals(storageEngine,
        DataSourceServiceImpl.getInstance()
            .getDataSource(DEFAULT_DATASOURCE_NAME).getStorageEngine());
    IllegalArgumentException illegalArgumentException
        = Assert.assertThrows(IllegalArgumentException.class,
          () -> DataSourceServiceImpl.getInstance().getDataSource("test"));
    Assert.assertEquals("DataSource with name test doesn't exist.",
        illegalArgumentException.getMessage());
  }


  @SneakyThrows
  @Test
  public void testGetStorageEngineAfterLoadingConnectors() {
    Settings settings = getDataSourceSettings("empty_datasource.json");
    DataSourceServiceImpl.getInstance().registerDefaultOpenSearchDataSource(storageEngine);
    //Load Connectors will empty the dataSourceMap.So OpenSearch Storage Engine
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    Set<DataSource> expected = new HashSet<>();
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithIllegalDataSourceNames() {
    Settings settings = getDataSourceSettings("illegal_datasource_name.json");
    Set<DataSource> expected = DataSourceServiceImpl.getInstance().getDataSources();
    DataSourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DataSourceServiceImpl.getInstance().getDataSources());
  }

  private Settings getDataSourceSettings(String filename) throws URISyntaxException, IOException {
    MockSecureSettings mockSecureSettings = new MockSecureSettings();
    ClassLoader classLoader = getClass().getClassLoader();
    Path filepath = Paths.get(classLoader.getResource(filename).toURI());
    mockSecureSettings.setFile(DATASOURCE_SETTING_METADATA_KEY, Files.readAllBytes(filepath));
    return Settings.builder().setSecureSettings(mockSecureSettings).build();
  }

}
