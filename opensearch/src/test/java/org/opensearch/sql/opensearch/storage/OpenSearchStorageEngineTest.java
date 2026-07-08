/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.system.OpenSearchCatalogTable;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;

@ExtendWith(MockitoExtension.class)
class OpenSearchStorageEngineTest {

  @Mock private OpenSearchClient client;

  @Mock private Settings settings;

  @Test
  public void getTable() {
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    Table table =
        engine.getTable(new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"), "test");
    assertAll(() -> assertNotNull(table), () -> assertTrue(table instanceof OpenSearchIndex));
  }

  @Test
  public void getFunctionsReturnsVectorSearchResolver() {
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    Collection<FunctionResolver> functions = engine.getFunctions();
    assertTrue(
        functions.stream().anyMatch(f -> f instanceof VectorSearchTableFunctionResolver),
        "getFunctions() should contain a VectorSearchTableFunctionResolver");
  }

  @Test
  public void getSystemTable() {
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    Table table =
        engine.getTable(new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"), TABLE_INFO);
    assertAll(
        () -> assertNotNull(table), () -> assertTrue(table instanceof OpenSearchCatalogTable));
  }

  @Test
  public void getRestTableAllowedByWildcard() {
    when(settings.getSettingValue(Settings.Key.PPL_REST_ALLOWED_ENDPOINTS))
        .thenReturn(List.of("*"));
    when(settings.getSettingValue(Settings.Key.PPL_REST_REDACTION_ENABLED)).thenReturn(false);
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    String name =
        SystemIndexUtils.restTable(
            new SystemIndexUtils.RestSpec("/_cat/nodes", Map.of(), null, null));
    Table table =
        engine.getTable(new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"), name);
    assertTrue(table instanceof OpenSearchCatalogTable);
  }

  @Test
  public void getRestTableAllowedBySubset() {
    when(settings.getSettingValue(Settings.Key.PPL_REST_ALLOWED_ENDPOINTS))
        .thenReturn(List.of("/_cat/nodes"));
    when(settings.getSettingValue(Settings.Key.PPL_REST_REDACTION_ENABLED)).thenReturn(false);
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    String name =
        SystemIndexUtils.restTable(
            new SystemIndexUtils.RestSpec("/_cat/nodes", Map.of(), null, null));
    assertTrue(
        engine.getTable(new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"), name)
            instanceof OpenSearchCatalogTable);
  }

  @Test
  public void getRestTableRejectedWhenEndpointNotInSubset() {
    when(settings.getSettingValue(Settings.Key.PPL_REST_ALLOWED_ENDPOINTS))
        .thenReturn(List.of("/_cat/nodes"));
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    String name =
        SystemIndexUtils.restTable(
            new SystemIndexUtils.RestSpec("/_cluster/settings", Map.of(), null, null));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                engine.getTable(
                    new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"), name));
    assertTrue(e.getMessage().contains("is not enabled on this cluster"));
  }

  @Test
  public void getRestTableDisabledWhenListEmpty() {
    when(settings.getSettingValue(Settings.Key.PPL_REST_ALLOWED_ENDPOINTS)).thenReturn(List.of());
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    String name =
        SystemIndexUtils.restTable(
            new SystemIndexUtils.RestSpec("/_cat/nodes", Map.of(), null, null));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                engine.getTable(
                    new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"), name));
    assertTrue(e.getMessage().contains("disabled on this cluster"));
  }
}
