/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.opensearch.sql.analysis.DatasourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

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
import org.opensearch.sql.datasource.model.Datasource;
import org.opensearch.sql.datasource.model.ConnectorType;
import org.opensearch.sql.storage.StorageEngine;

@RunWith(MockitoJUnitRunner.class)
public class DatasourceServiceImplTest {

  public static final String CATALOG_SETTING_METADATA_KEY =
      "plugins.query.federation.catalog.config";

  @Mock
  private StorageEngine storageEngine;

  @SneakyThrows
  @Test
  public void testLoadConnectors() {
    Settings settings = getCatalogSettings("catalogs.json");
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    Set<Datasource> expected = new HashSet<>() {{
        add(new Datasource("prometheus", ConnectorType.PROMETHEUS, storageEngine));
      }};
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
  }


  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMultipleCatalogs() {
    Settings settings = getCatalogSettings("multiple_catalogs.json");
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    Set<Datasource> expected = new HashSet<>() {{
        add(new Datasource("prometheus", ConnectorType.PROMETHEUS, storageEngine));
        add(new Datasource("prometheus-1", ConnectorType.PROMETHEUS, storageEngine));
      }};
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMissingName() {
    Settings settings = getCatalogSettings("catalog_missing_name.json");
    Set<Datasource> expected = DatasourceServiceImpl.getInstance().getDatasources();
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithDuplicateCatalogNames() {
    Settings settings = getCatalogSettings("duplicate_catalog_names.json");
    Set<Datasource> expected = DatasourceServiceImpl.getInstance().getDatasources();
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMalformedJson() {
    Settings settings = getCatalogSettings("malformed_catalogs.json");
    Set<Datasource> expected = DatasourceServiceImpl.getInstance().getDatasources();
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
  }

  @SneakyThrows
  @Test
  public void testGetStorageEngineAfterGetCatalogs() {
    Settings settings = getCatalogSettings("empty_catalog.json");
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    DatasourceServiceImpl.getInstance().registerDefaultOpenSearchDatasource(storageEngine);
    Set<Datasource> expected = new HashSet<>();
    expected.add(new Datasource(DEFAULT_DATASOURCE_NAME, ConnectorType.OPENSEARCH, storageEngine));
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
    Assert.assertEquals(storageEngine,
        DatasourceServiceImpl.getInstance().getDatasource(DEFAULT_DATASOURCE_NAME).getStorageEngine());
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
    Assert.assertEquals(storageEngine,
        DatasourceServiceImpl.getInstance().getDatasource(DEFAULT_DATASOURCE_NAME).getStorageEngine());
    IllegalArgumentException illegalArgumentException
        = Assert.assertThrows(IllegalArgumentException.class,
          () -> DatasourceServiceImpl.getInstance().getDatasource("test"));
    Assert.assertEquals("Catalog with name test doesn't exist.",
        illegalArgumentException.getMessage());
  }


  @SneakyThrows
  @Test
  public void testGetStorageEngineAfterLoadingConnectors() {
    Settings settings = getCatalogSettings("empty_catalog.json");
    DatasourceServiceImpl.getInstance().registerDefaultOpenSearchDatasource(storageEngine);
    //Load Connectors will empty the catalogMap.So OpenSearch Storage Engine
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    Set<Datasource> expected = new HashSet<>();
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithIllegalCatalogNames() {
    Settings settings = getCatalogSettings("illegal_catalog_name.json");
    Set<Datasource> expected = DatasourceServiceImpl.getInstance().getDatasources();
    DatasourceServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, DatasourceServiceImpl.getInstance().getDatasources());
  }

  private Settings getCatalogSettings(String filename) throws URISyntaxException, IOException {
    MockSecureSettings mockSecureSettings = new MockSecureSettings();
    ClassLoader classLoader = getClass().getClassLoader();
    Path filepath = Paths.get(classLoader.getResource(filename).toURI());
    mockSecureSettings.setFile(CATALOG_SETTING_METADATA_KEY, Files.readAllBytes(filepath));
    return Settings.builder().setSecureSettings(mockSecureSettings).build();
  }

}
