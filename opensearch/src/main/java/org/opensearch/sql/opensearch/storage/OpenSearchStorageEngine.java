/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  @Getter
  private final OpenSearchClient client;
  @Getter
  private final Settings settings;

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName,
                        String name,
                        @Nullable String routingId) {
    if (isSystemIndex(name)) {
      // TODO: handle routingId on system tables too?
      return new OpenSearchSystemIndex(client, name);
    } else {
      return new OpenSearchIndex(client, settings, name, routingId);
    }
  }
}
