/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

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
    Set<String> expected = new HashSet<>() {{
        add("prometheus");
      }};
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }


  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMultipleCatalogs() {
    Settings settings = getCatalogSettings("multiple_catalogs.json");
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Set<String> expected = new HashSet<>() {{
        add("prometheus");
        add("prometheus-1");
      }};
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMissingName() {
    Settings settings = getCatalogSettings("catalog_missing_name.json");
    Assert.assertThrows(IllegalArgumentException.class,
        () -> CatalogServiceImpl.getInstance().loadConnectors(settings));
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithDuplicateCatalogNames() {
    Settings settings = getCatalogSettings("duplicate_catalog_names.json");
    Assert.assertThrows(IllegalArgumentException.class,
        () -> CatalogServiceImpl.getInstance().loadConnectors(settings));
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMalformedJson() {
    Settings settings = getCatalogSettings("malformed_catalogs.json");
    Assert.assertThrows(IllegalArgumentException.class,
        () -> CatalogServiceImpl.getInstance().loadConnectors(settings));
  }

  @SneakyThrows
  @Test
  public void testGetStorageEngineAfterGetCatalogs() {
    Settings settings = getCatalogSettings("empty_catalog.json");
    CatalogServiceImpl.getInstance().registerOpenSearchStorageEngine(storageEngine);
    CatalogServiceImpl.getInstance().loadConnectors(settings);
    Set<String> expected = new HashSet<>();
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
    Assert.assertEquals(storageEngine, CatalogServiceImpl.getInstance().getStorageEngine(null));
    Assert.assertEquals(expected, CatalogServiceImpl.getInstance().getCatalogs());
    Assert.assertEquals(storageEngine, CatalogServiceImpl.getInstance().getStorageEngine(null));
  }


  private Settings getCatalogSettings(String filename) throws URISyntaxException, IOException {
    MockSecureSettings mockSecureSettings = new MockSecureSettings();
    ClassLoader classLoader = getClass().getClassLoader();
    Path filepath = Paths.get(classLoader.getResource(filename).toURI());
    mockSecureSettings.setFile(CATALOG_SETTING_METADATA_KEY, Files.readAllBytes(filepath));
    return Settings.builder().setSecureSettings(mockSecureSettings).build();
  }

}
