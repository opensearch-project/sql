/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.*;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch connection by node client. */
public class OpenSearchNodeClient implements OpenSearchClient {

  public static final Function<String, Predicate<String>> ALL_FIELDS =
      (anyIndex -> (anyField -> true));

  /** Node client provided by OpenSearch container. */
  private final NodeClient client;

  /** Constructor of OpenSearchNodeClient. */
  public OpenSearchNodeClient(NodeClient client) {
    this.client = client;
  }

  @Override
  public boolean exists(String indexName) {
    try {
      IndicesExistsResponse checkExistResponse =
          client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
      return checkExistResponse.isExists();
    } catch (OpenSearchSecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to check if index [" + indexName + "] exists", e);
    }
  }

  @Override
  public void createIndex(String indexName, Map<String, Object> mappings) {
    try {
      // TODO: 1.pass index settings (the number of primary shards, etc); 2.check response?
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).mapping(mappings);
      client.admin().indices().create(createIndexRequest).actionGet();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create index [" + indexName + "]", e);
    }
  }

  /**
   * Get field mappings of index by an index expression. Majority is copied from legacy
   * LocalClusterState.
   *
   * <p>For simplicity, removed type (deprecated) and field filter in argument list. Also removed
   * mapping cache, cluster state listener (mainly for performance and debugging).
   *
   * @param indexExpression index name expression
   * @return index mapping(s) in our class to isolate OpenSearch API. IndexNotFoundException is
   *     thrown if no index matched.
   */
  @Override
  public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {
    try {
      GetMappingsResponse mappingsResponse =
          client.admin().indices().prepareGetMappings(indexExpression).setLocal(true).get();
      if (mappingsResponse.mappings().isEmpty()) {
        throw new IndexNotFoundException(indexExpression[0]);
      }
      return mappingsResponse.mappings().entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  Map.Entry::getKey, cursor -> new IndexMapping(cursor.getValue())));
    } catch (IndexNotFoundException e) {
      // Re-throw directly to be treated as client error finally
      throw ErrorReport.wrap(e)
          .code(ErrorCode.INDEX_NOT_FOUND)
          .location("while fetching index mappings")
          .context("index_name", indexExpression[0])
          .build();
    } catch (OpenSearchSecurityException e) {
      // Re-throw with permission denied code
      throw ErrorReport.wrap(e)
          .code(ErrorCode.PERMISSION_DENIED)
          .location("while fetching index mappings")
          .context("index_name", indexExpression[0])
          .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read mapping for index pattern ["
              + String.join(",", indexExpression)
              + "]: "
              + e.getMessage(),
          e);
    }
  }

  /**
   * Fetch index.max_result_window settings according to index expression given.
   *
   * @param indexExpression index expression
   * @return map from index name to its max result window
   */
  @Override
  public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {
    try {
      GetSettingsResponse settingsResponse =
          client.admin().indices().prepareGetSettings(indexExpression).setLocal(true).get();
      ImmutableMap.Builder<String, Integer> result = ImmutableMap.builder();
      for (Map.Entry<String, Settings> indexToSetting :
          settingsResponse.getIndexToSettings().entrySet()) {
        Settings settings = indexToSetting.getValue();
        result.put(
            indexToSetting.getKey(),
            settings.getAsInt(
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(settings)));
      }
      return result.build();
    } catch (OpenSearchSecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read setting for index pattern ["
              + String.join(",", indexExpression)
              + "]: "
              + e.getMessage(),
          e);
    }
  }

  /** TODO: Scroll doesn't work for aggregation. Support aggregation later. */
  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    return request.search(
        req -> client.search(req).actionGet(), req -> client.searchScroll(req).actionGet());
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    final GetIndexResponse indexResponse =
        client.admin().indices().prepareGetIndex().setLocal(true).get();
    final Stream<String> aliasStream =
        ImmutableList.copyOf(indexResponse.aliases().values()).stream()
            .flatMap(Collection::stream)
            .map(AliasMetadata::alias);

    return Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
        .collect(Collectors.toList());
  }

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  @Override
  public Map<String, String> meta() {
    return ImmutableMap.of(
        META_CLUSTER_NAME,
        client.settings().get("cluster.name", "opensearch"),
        "plugins.sql.pagination.api",
        client.settings().get("plugins.sql.pagination.api", "true"));
  }

  @Override
  public void forceCleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.forceClean(
          scrollId -> {
            try {
              client.prepareClearScroll().addScrollId(scrollId).get();
            } catch (Exception e) {
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else {
      request.forceClean(
          pitId -> {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            deletePit(deletePitRequest);
          });
    }
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.clean(
          scrollId -> {
            try {
              client.prepareClearScroll().addScrollId(scrollId).get();
            } catch (Exception e) {
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else {
      request.clean(
          pitId -> {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            deletePit(deletePitRequest);
          });
    }
  }

  @Override
  public void schedule(Runnable task) {
    // at that time, task already running the sql-worker ThreadPool.
    task.run();
  }

  @Override
  public Optional<NodeClient> getNodeClient() {
    return Optional.of(client);
  }

  @Override
  public String createPit(CreatePitRequest createPitRequest) {
    ActionFuture<CreatePitResponse> execute =
        this.client.execute(CreatePitAction.INSTANCE, createPitRequest);
    try {
      CreatePitResponse pitResponse = execute.get();
      return pitResponse.getId();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof OpenSearchSecurityException) {
        throw (OpenSearchSecurityException) e.getCause();
      }
      throw new RuntimeException(
          "Error occurred while creating PIT for internal plugin operation", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Error occurred while creating PIT for internal plugin operation", e);
    }
  }

  @Override
  public void deletePit(DeletePitRequest deletePitRequest) {
    ActionFuture<DeletePitResponse> execute =
        this.client.execute(DeletePitAction.INSTANCE, deletePitRequest);
    try {
      execute.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof OpenSearchSecurityException) {
        throw (OpenSearchSecurityException) e.getCause();
      }
      throw new RuntimeException(
          "Error occurred while deleting PIT for internal plugin operation", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Error occurred while deleting PIT for internal plugin operation", e);
    }
  }

  @Override
  public Map<String, Object> clusterHealth(Map<String, String> params) {
    ClusterHealthRequest request = new ClusterHealthRequest();
    if (params != null && Boolean.parseBoolean(params.get("local"))) {
      request.local(true);
    }
    ClusterHealthResponse response = client.admin().cluster().health(request).actionGet();
    return flattenHealth(response);
  }

  @Override
  public List<Map<String, Object>> catIndices(Map<String, String> params) {
    ClusterHealthResponse response =
        client.admin().cluster().health(new ClusterHealthRequest()).actionGet();
    List<Map<String, Object>> rows = new java.util.ArrayList<>();
    for (Map.Entry<String, ClusterIndexHealth> entry : response.getIndices().entrySet()) {
      ClusterIndexHealth health = entry.getValue();
      Map<String, Object> row = new java.util.LinkedHashMap<>();
      row.put("index", entry.getKey());
      row.put("health", health.getStatus().name().toLowerCase(java.util.Locale.ROOT));
      row.put("pri", health.getNumberOfShards());
      row.put("rep", health.getNumberOfReplicas());
      row.put("active_shards", health.getActiveShards());
      rows.add(row);
    }
    String healthFilter = params == null ? null : params.get("health");
    if (healthFilter != null) {
      rows.removeIf(r -> !healthFilter.equalsIgnoreCase(String.valueOf(r.get("health"))));
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> catNodes(Map<String, String> params) {
    org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest statsRequest =
        new org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest();
    statsRequest.all();
    org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse response =
        client.admin().cluster().nodesStats(statsRequest).actionGet();
    List<Map<String, Object>> rows = new java.util.ArrayList<>();
    for (org.opensearch.action.admin.cluster.node.stats.NodeStats ns : response.getNodes()) {
      org.opensearch.cluster.node.DiscoveryNode node = ns.getNode();
      Map<String, Object> row = new java.util.LinkedHashMap<>();
      row.put("name", node.getName());
      row.put("ip", node.getHostAddress());
      row.put(
          "node_role",
          node.getRoles().stream()
              .map(org.opensearch.cluster.node.DiscoveryNodeRole::roleName)
              .sorted()
              .collect(java.util.stream.Collectors.joining(",")));
      row.put(
          "heap_percent",
          ns.getJvm() == null || ns.getJvm().getMem() == null
              ? null
              : (int) ns.getJvm().getMem().getHeapUsedPercent());
      row.put(
          "ram_percent",
          ns.getOs() == null || ns.getOs().getMem() == null
              ? null
              : (int) ns.getOs().getMem().getUsedPercent());
      row.put(
          "cpu",
          ns.getProcess() == null || ns.getProcess().getCpu() == null
              ? null
              : (int) ns.getProcess().getCpu().getPercent());
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> catClusterManager(Map<String, String> params) {
    org.opensearch.action.admin.cluster.state.ClusterStateResponse response =
        client
            .admin()
            .cluster()
            .state(new org.opensearch.action.admin.cluster.state.ClusterStateRequest())
            .actionGet();
    org.opensearch.cluster.node.DiscoveryNode cm =
        response.getState().nodes().getClusterManagerNode();
    List<Map<String, Object>> rows = new java.util.ArrayList<>();
    if (cm != null) {
      Map<String, Object> row = new java.util.LinkedHashMap<>();
      row.put("id", cm.getId());
      row.put("host", cm.getHostName());
      row.put("ip", cm.getHostAddress());
      row.put("node", cm.getName());
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> catPlugins(Map<String, String> params) {
    org.opensearch.action.admin.cluster.node.info.NodesInfoRequest infoRequest =
        new org.opensearch.action.admin.cluster.node.info.NodesInfoRequest();
    infoRequest.all();
    org.opensearch.action.admin.cluster.node.info.NodesInfoResponse response =
        client.admin().cluster().nodesInfo(infoRequest).actionGet();
    List<Map<String, Object>> rows = new java.util.ArrayList<>();
    for (org.opensearch.action.admin.cluster.node.info.NodeInfo info : response.getNodes()) {
      org.opensearch.action.admin.cluster.node.info.PluginsAndModules plugins =
          info.getInfo(org.opensearch.action.admin.cluster.node.info.PluginsAndModules.class);
      if (plugins == null) {
        continue;
      }
      for (org.opensearch.plugins.PluginInfo pi : plugins.getPluginInfos()) {
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("name", info.getNode().getName());
        row.put("component", pi.getName());
        row.put("version", pi.getVersion());
        rows.add(row);
      }
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> catShards(Map<String, String> params) {
    org.opensearch.action.admin.cluster.state.ClusterStateResponse response =
        client
            .admin()
            .cluster()
            .state(new org.opensearch.action.admin.cluster.state.ClusterStateRequest())
            .actionGet();
    org.opensearch.cluster.node.DiscoveryNodes nodes = response.getState().nodes();
    List<Map<String, Object>> rows = new java.util.ArrayList<>();
    for (org.opensearch.cluster.routing.ShardRouting sr :
        response.getState().getRoutingTable().allShards()) {
      Map<String, Object> row = new java.util.LinkedHashMap<>();
      row.put("index", sr.getIndexName());
      row.put("shard", sr.id());
      row.put("prirep", sr.primary() ? "p" : "r");
      row.put("state", sr.state().name());
      org.opensearch.cluster.node.DiscoveryNode n =
          sr.currentNodeId() == null ? null : nodes.get(sr.currentNodeId());
      row.put("node", n == null ? null : n.getName());
      rows.add(row);
    }
    return rows;
  }

  @Override
  public Map<String, Object> clusterState(Map<String, String> params) {
    org.opensearch.action.admin.cluster.state.ClusterStateResponse response =
        client
            .admin()
            .cluster()
            .state(new org.opensearch.action.admin.cluster.state.ClusterStateRequest())
            .actionGet();
    Map<String, Object> row = new java.util.LinkedHashMap<>();
    row.put("cluster_name", response.getClusterName().value());
    row.put("state_uuid", response.getState().stateUUID());
    row.put("version", response.getState().version());
    org.opensearch.cluster.node.DiscoveryNode cm =
        response.getState().nodes().getClusterManagerNode();
    row.put("cluster_manager_node", cm == null ? null : cm.getName());
    return row;
  }

  @Override
  public List<Map<String, Object>> clusterSettings(Map<String, String> params) {
    // The transport path has no SettingsFilter of its own; it is published from
    // SQLPlugin#getRestHandlers at startup. Fail closed before fetching so we never read settings
    // into memory when we cannot redact them, matching native GET /_cluster/settings.
    org.opensearch.common.settings.SettingsFilter filter =
        org.opensearch.sql.opensearch.storage.rest.RestSettingsFilterHolder.get();
    if (filter == null) {
      throw new IllegalStateException(
          "cluster settings redaction filter is not initialized; refusing to return unredacted"
              + " settings");
    }
    org.opensearch.action.admin.cluster.state.ClusterStateResponse response =
        client
            .admin()
            .cluster()
            .state(new org.opensearch.action.admin.cluster.state.ClusterStateRequest())
            .actionGet();
    List<Map<String, Object>> rows = new java.util.ArrayList<>();
    org.opensearch.common.settings.Settings persistent =
        filter.filter(response.getState().metadata().persistentSettings());
    org.opensearch.common.settings.Settings transientSettings =
        filter.filter(response.getState().metadata().transientSettings());
    collectSettings(persistent, "persistent", rows);
    collectSettings(transientSettings, "transient", rows);
    return rows;
  }

  private void collectSettings(
      org.opensearch.common.settings.Settings settings,
      String tier,
      List<Map<String, Object>> rows) {
    if (settings == null) {
      return;
    }
    for (String key : settings.keySet()) {
      Map<String, Object> row = new java.util.LinkedHashMap<>();
      row.put("setting", key);
      String value = settings.get(key);
      if (value == null) {
        // List-valued settings return null from get(); fall back to the joined list form.
        java.util.List<String> list = settings.getAsList(key);
        value = list.isEmpty() ? null : String.join(",", list);
      }
      row.put("value", value);
      row.put("tier", tier);
      rows.add(row);
    }
  }

  @Override
  public List<Map<String, Object>> resolveIndex(Map<String, String> params) {
    String expandWildcards = params == null ? null : params.get("expand_wildcards");
    org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Request request =
        expandWildcards == null
            ? new org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Request(
                new String[] {"*"})
            : new org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Request(
                new String[] {"*"},
                org.opensearch.action.support.IndicesOptions.fromParameters(
                    expandWildcards,
                    null,
                    null,
                    null,
                    org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Request
                        .DEFAULT_INDICES_OPTIONS));
    org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Response response =
        client
            .execute(
                org.opensearch.action.admin.indices.resolve.ResolveIndexAction.INSTANCE, request)
            .actionGet();
    List<Map<String, Object>> rows = new java.util.ArrayList<>();
    for (org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex idx :
        response.getIndices()) {
      rows.add(resolveRow(idx.getName(), "index"));
    }
    for (org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedAlias alias :
        response.getAliases()) {
      rows.add(resolveRow(alias.getName(), "alias"));
    }
    for (org.opensearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream ds :
        response.getDataStreams()) {
      rows.add(resolveRow(ds.getName(), "data_stream"));
    }
    return rows;
  }

  private Map<String, Object> resolveRow(String name, String type) {
    Map<String, Object> row = new java.util.LinkedHashMap<>();
    row.put("name", name);
    row.put("type", type);
    return row;
  }

  private Map<String, Object> flattenHealth(ClusterHealthResponse response) {
    Map<String, Object> row = new java.util.LinkedHashMap<>();
    row.put("cluster_name", response.getClusterName());
    row.put("status", response.getStatus().name().toLowerCase(java.util.Locale.ROOT));
    row.put("number_of_nodes", response.getNumberOfNodes());
    row.put("number_of_data_nodes", response.getNumberOfDataNodes());
    row.put("active_primary_shards", response.getActivePrimaryShards());
    row.put("active_shards", response.getActiveShards());
    row.put("relocating_shards", response.getRelocatingShards());
    row.put("initializing_shards", response.getInitializingShards());
    row.put("unassigned_shards", response.getUnassignedShards());
    row.put("timed_out", response.isTimedOut());
    return row;
  }
}
