/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  @Getter private final OpenSearchClient client;

  @Getter private final Settings settings;

  // Temporary flag to allow permissive mode only in integ test
  private final boolean supportPermissiveMode;

  public OpenSearchStorageEngine(OpenSearchClient client, Settings settings) {
    this(client, settings, false);
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    if (isSystemIndex(name)) {
      return new OpenSearchSystemIndex(client, settings, name);
    } else {
      return new OpenSearchIndex(client, settings, name);
    }
  }

  @Override
  public Table getPermissiveAwareTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    if (isSystemIndex(name)) {
      return new OpenSearchSystemIndex(client, settings, name);
    } else if (supportPermissiveMode && isPermissiveEnabled()) {
      return new PermissiveOpenSearchIndex(client, settings, name);
    } else {
      return new OpenSearchIndex(client, settings, name);
    }
  }

  private boolean isPermissiveEnabled() {
    if (settings != null) {
      return settings.getSettingValue(Key.PPL_QUERY_PERMISSIVE);
    } else {
      return false;
    }
  }
}
