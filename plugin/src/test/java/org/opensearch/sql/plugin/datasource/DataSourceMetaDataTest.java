/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.plugin.SQLPlugin;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceMetaDataTest {

  public static final String DATASOURCE_SETTING_METADATA_KEY =
      "plugins.query.federation.datasources.config";

  @Mock
  private DataSourceService dataSourceService;

  @SneakyThrows
  @Test
  public void testLoadConnectors() {
    Settings settings = getDataSourceSettings("datasources.json");
    loadConnectors(settings);
    List<DataSourceMetadata> expected =
        new ArrayList<>() {
          {
            add(
                metadata(
                    "prometheus",
                    DataSourceType.PROMETHEUS,
                    ImmutableMap.of(
                        "prometheus.uri", "http://localhost:9090",
                        "prometheus.auth.type", "basicauth",
                        "prometheus.auth.username", "admin",
                        "prometheus.auth.password", "type")));
          }
        };

    verifyAddDataSourceWithMetadata(expected);
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMultipleDataSources() {
    Settings settings = getDataSourceSettings("multiple_datasources.json");
    loadConnectors(settings);
    List<DataSourceMetadata> expected = new ArrayList<>() {{
        add(metadata("prometheus", DataSourceType.PROMETHEUS, ImmutableMap.of(
            "prometheus.uri", "http://localhost:9090",
            "prometheus.auth.type", "basicauth",
            "prometheus.auth.username", "admin",
            "prometheus.auth.password", "type"
        )));
        add(metadata("prometheus-1", DataSourceType.PROMETHEUS, ImmutableMap.of(
            "prometheus.uri", "http://localhost:9090",
            "prometheus.auth.type", "awssigv4",
            "prometheus.auth.region", "us-east-1",
            "prometheus.auth.access_key", "accessKey",
            "prometheus.auth.secret_key", "secretKey"
        )));
      }};

    verifyAddDataSourceWithMetadata(expected);
  }

  @SneakyThrows
  @Test
  public void testLoadConnectorsWithMalformedJson() {
    Settings settings = getDataSourceSettings("malformed_datasources.json");
    loadConnectors(settings);

    verify(dataSourceService, never()).addDataSource(any());
  }

  private Settings getDataSourceSettings(String filename) throws URISyntaxException, IOException {
    MockSecureSettings mockSecureSettings = new MockSecureSettings();
    ClassLoader classLoader = getClass().getClassLoader();
    Path filepath = Paths.get(classLoader.getResource(filename).toURI());
    mockSecureSettings.setFile(DATASOURCE_SETTING_METADATA_KEY, Files.readAllBytes(filepath));
    return Settings.builder().setSecureSettings(mockSecureSettings).build();
  }

  void loadConnectors(Settings settings) {
    SQLPlugin.loadDataSources(dataSourceService, settings);
  }

  void verifyAddDataSourceWithMetadata(List<DataSourceMetadata> metadataList) {
    ArgumentCaptor<DataSourceMetadata[]> metadataCaptor =
        ArgumentCaptor.forClass(DataSourceMetadata[].class);
    verify(dataSourceService, times(1)).addDataSource(metadataCaptor.capture());
    List<DataSourceMetadata> actualValues = Arrays.asList(metadataCaptor.getValue());
    assertEquals(metadataList.size(), actualValues.size());
    assertEquals(metadataList, actualValues);
  }

  DataSourceMetadata metadata(String name, DataSourceType type, Map<String, String> properties) {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName(name);
    dataSourceMetadata.setConnector(type);
    dataSourceMetadata.setProperties(properties);
    return dataSourceMetadata;
  }
}
