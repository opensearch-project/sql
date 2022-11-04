/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import static org.opensearch.sql.analysis.CatalogSchemaIdentifierNameResolver.DEFAULT_CATALOG_NAME;

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
import org.opensearch.sql.catalog.model.Catalog;
import org.opensearch.sql.catalog.model.ConnectorType;
import org.opensearch.sql.storage.StorageEngine;

@RunWith(MockitoJUnitRunner.class)
public class CatalogServiceImplTest {

  public static final String CATALOG_SETTING_METADATA_KEY =
      "plugins.query.federation.catalog.config";

  @Mock
  private StorageEngine storageEngine;

  @SneakyThrows
  @Test
  public void testLoadConnectors() {
    Settings settings = getCatalogSettings("catalogs.json");
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Set<Catalog> expected = new HashSet<>() {{
        add(new Catalog("prometheus", ConnectorType.PROMETHEUS, storageEngine));
      }};
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }


  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMultipleCatalogs() {
    Settings settings = getCatalogSettings("multiple_catalogs.json");
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Set<Catalog> expected = new HashSet<>() {{
        add(new Catalog("prometheus", ConnectorType.PROMETHEUS, storageEngine));
        add(new Catalog("prometheus-1", ConnectorType.PROMETHEUS, storageEngine));
      }};
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMissingName() {
    Settings settings = getCatalogSettings("catalog_missing_name.json");
    Set<Catalog> expected = CatalogServiceImpl.getInstance().getCatalogs();
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithDuplicateCatalogNames() {
    Settings settings = getCatalogSettings("duplicate_catalog_names.json");
    Set<Catalog> expected = CatalogServiceImpl.getInstance().getCatalogs();
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMalformedJson() {
    Settings settings = getCatalogSettings("malformed_catalogs.json");
    Set<Catalog> expected = CatalogServiceImpl.getInstance().getCatalogs();
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }

  @SneakyThrows
  @Test
  public void testGetStorageEngineAfterGetCatalogs() {
    Settings settings = getCatalogSettings("empty_catalog.json");
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    CatalogServiceImpl.getInstance().registerDefaultOpenSearchCatalog(storageEngine);
    Set<Catalog> expected = new HashSet<>();
    expected.add(new Catalog(DEFAULT_CATALOG_NAME, ConnectorType.OPENSEARCH, storageEngine));
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
    Assert.assertEquals(storageEngine,
        CatalogServiceImpl.getInstance().getCatalog(DEFAULT_CATALOG_NAME).getStorageEngine());
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
    Assert.assertEquals(storageEngine,
        CatalogServiceImpl.getInstance().getCatalog(DEFAULT_CATALOG_NAME).getStorageEngine());
    IllegalArgumentException illegalArgumentException
        = Assert.assertThrows(IllegalArgumentException.class,
          () -> CatalogServiceImpl.getInstance().getCatalog("test"));
    Assert.assertEquals("Catalog with name test doesn't exist.",
        illegalArgumentException.getMessage());
  }


  @SneakyThrows
  @Test
  public void testGetStorageEngineAfterLoadingConnectors() {
    Settings settings = getCatalogSettings("empty_catalog.json");
    CatalogServiceImpl.getInstance().registerDefaultOpenSearchCatalog(storageEngine);
    //Load Connectors will empty the catalogMap.So OpenSearch Storage Engine
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Set<Catalog> expected = new HashSet<>();
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithIllegalCatalogNames() {
    Settings settings = getCatalogSettings("illegal_catalog_name.json");
    Set<Catalog> expected = CatalogServiceImpl.getInstance().getCatalogs();
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }

  private Settings getCatalogSettings(String filename) throws URISyntaxException, IOException {
    MockSecureSettings mockSecureSettings = new MockSecureSettings();
    ClassLoader classLoader = getClass().getClassLoader();
    Path filepath = Paths.get(classLoader.getResource(filename).toURI());
    mockSecureSettings.setFile(CATALOG_SETTING_METADATA_KEY, Files.readAllBytes(filepath));
    return Settings.builder().setSecureSettings(mockSecureSettings).build();
  }

}
