/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.decodeRestSpec;
import static org.opensearch.sql.utils.SystemIndexUtils.isRestSource;
import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import java.util.Collection;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.rest.RestCatalogSource;
import org.opensearch.sql.opensearch.storage.rest.RestEndpointRegistry;
import org.opensearch.sql.opensearch.storage.system.OpenSearchCatalogTable;
import org.opensearch.sql.opensearch.storage.system.SystemIndexCatalogSource;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  @Getter private final OpenSearchClient client;

  @Getter private final Settings settings;

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return List.of(new VectorSearchTableFunctionResolver(client, settings));
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    if (isRestSource(name)) {
      return restTable(name);
    } else if (isSystemIndex(name)) {
      return new OpenSearchCatalogTable(new SystemIndexCatalogSource(client, name), settings);
    } else {
      return new OpenSearchIndex(client, settings, name);
    }
  }

  private Table restTable(String name) {
    RestSpec spec = decodeRestSpec(name);
    RestEndpointRegistry.resolve(spec.getEndpoint());
    List<String> allowed = settings.getSettingValue(Settings.Key.PPL_REST_ALLOWED_ENDPOINTS);
    if (allowed == null || !(allowed.contains("*") || allowed.contains(spec.getEndpoint()))) {
      throw new IllegalArgumentException(
          allowed == null || allowed.isEmpty()
              ? "the rest command is disabled on this cluster"
              : "rest endpoint ["
                  + spec.getEndpoint()
                  + "] is not enabled on this cluster. Enabled endpoints: "
                  + allowed);
    }
    boolean redact = settings.getSettingValue(Settings.Key.PPL_REST_REDACTION_ENABLED);
    return new OpenSearchCatalogTable(new RestCatalogSource(client, spec, redact), settings);
  }
}
