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
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.TimeBounds;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.IndexPruner;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.storage.rest.RestCatalogSource;
import org.opensearch.sql.opensearch.storage.rest.RestEndpointRegistry;
import org.opensearch.sql.opensearch.storage.rest.RestEndpointRegistryHolder;
import org.opensearch.sql.opensearch.storage.system.OpenSearchCatalogTable;
import org.opensearch.sql.opensearch.storage.system.SystemIndexCatalogSource;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.SupportsIndexPruning;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
@Log4j2
public class OpenSearchStorageEngine implements StorageEngine, SupportsIndexPruning {

  /** OpenSearch client connection. */
  @Getter private final OpenSearchClient client;

  @Getter private final Settings settings;

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return List.of(new VectorSearchTableFunctionResolver(client, settings));
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    return getTable(dataSourceSchemaName, name, null);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Narrowing happens here, as the table is built, rather than on the table: an index's schema
   * is the merge of the mapping of every index its name resolves to, so once a table exists that
   * merge has already been paid. Only a concrete index expression can be narrowed -- a catalog or
   * REST source has no indices to skip.
   */
  @Override
  public Table getTable(
      DataSourceSchemaName dataSourceSchemaName, String name, @Nullable TimeBounds bounds) {
    if (isRestSource(name)) {
      return restTable(name);
    } else if (isSystemIndex(name)) {
      return new OpenSearchCatalogTable(new SystemIndexCatalogSource(client, name), settings);
    } else {
      return new OpenSearchIndex(client, settings, prune(name, bounds));
    }
  }

  /**
   * The index expression to read: those of {@code name}'s indices that can hold data in {@code
   * bounds}, or {@code name} itself when pruning is off, declined or unnecessary.
   *
   * <p>Only the node client can issue the probes, so a query through the REST client never prunes.
   */
  private String prune(String name, @Nullable TimeBounds bounds) {
    if (bounds == null
        || !Boolean.TRUE.equals(settings.getSettingValue(Settings.Key.QUERY_PRUNING_ENABLED))) {
      return name;
    }
    return client
        .getNodeClient()
        .map(
            node -> {
              String pruned =
                  new IndexPruner(node)
                      .prune(new OpenSearchRequest.IndexName(name), bounds)
                      .toString();
              if (!pruned.equals(name)) {
                log.info("Pruned index expression from {} to {}", name, pruned);
              }
              return pruned;
            })
        .orElse(name);
  }

  private Table restTable(String name) {
    RestSpec spec = decodeRestSpec(name);
    RestEndpointRegistry registry = RestEndpointRegistryHolder.get();
    registry.resolve(spec.getEndpoint());
    List<String> allowed = settings.getSettingValue(Settings.Key.PPL_REST_ALLOWED_ENDPOINTS);
    if (allowed == null || !allowed.contains(spec.getEndpoint())) {
      throw new IllegalArgumentException(
          allowed == null || allowed.isEmpty()
              ? "the rest command is disabled on this cluster"
              : "rest endpoint ["
                  + spec.getEndpoint()
                  + "] is not enabled on this cluster. Enabled endpoints: "
                  + allowed);
    }
    return new OpenSearchCatalogTable(new RestCatalogSource(registry, spec, client), settings);
  }
}
