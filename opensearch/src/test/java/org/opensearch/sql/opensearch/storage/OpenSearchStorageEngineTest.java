/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class OpenSearchStorageEngineTest {

  @Mock
  private OpenSearchClient client;

  @Mock
  private Settings settings;

  @Test
  public void getTable() {
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    Table table = engine.getTable(new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"),
        "test");
    assertAll(
        () -> assertNotNull(table),
        () -> assertTrue(table instanceof OpenSearchIndex)
    );
  }

  @Test
  public void getSystemTable() {
    OpenSearchStorageEngine engine = new OpenSearchStorageEngine(client, settings);
    Table table = engine.getTable(new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"),
        TABLE_INFO);
    assertAll(
        () -> assertNotNull(table),
        () -> assertTrue(table instanceof OpenSearchSystemIndex)
    );
  }
}
