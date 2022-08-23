/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ddl.Column;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  private final Settings settings;

  @Override
  public Table getTable(String name) {
    if (isSystemIndex(name)) {
      return new OpenSearchSystemIndex(client, name);
    } else {
      return new OpenSearchIndex(client, settings, name);
    }
  }

  @Override
  public boolean addTable(String name, List<Column> columns) {
    Map<String, Object> properties = new HashMap<>();
    for (Column column : columns) {
      Map<String, Object> field = new HashMap<>();
      String fieldType = column.getType();
      if (fieldType.equals("string")) {
        field.put("type", "keyword");
      } else if (fieldType.equals("date")) {
        field.put("format",  "strict_date_optional_time||epoch_second");
        field.put("type", fieldType);
      } else {
        field.put("type", fieldType);
      }
      properties.put(column.getName(), field); // TODO: convert SQL type to ES type in analyzer
    }

    Map<String, Object> mapping = new HashMap<>();
    mapping.put("properties", properties);
    return client.createIndex(name, mapping);
  }
}
