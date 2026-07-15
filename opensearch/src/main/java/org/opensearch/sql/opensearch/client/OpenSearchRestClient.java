/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.*;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.transport.client.node.NodeClient;

/**
 * OpenSearch REST client to support standalone mode that runs entire engine from remote.
 *
 * <p>TODO: Support for authN and authZ with AWS Sigv4 or security plugin.
 */
@RequiredArgsConstructor
public class OpenSearchRestClient implements OpenSearchClient {

  /** OpenSearch high level REST client. */
  private final RestHighLevelClient client;

  @Override
  public boolean exists(String indexName) {
    try {
      return client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index [" + indexName + "] exist", e);
    }
  }

  @Override
  public void createIndex(String indexName, Map<String, Object> mappings) {
    try {
      client
          .indices()
          .create(new CreateIndexRequest(indexName).mapping(mappings), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create index [" + indexName + "]", e);
    }
  }

  @Override
  public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {
    GetMappingsRequest request = new GetMappingsRequest().indices(indexExpression);
    try {
      GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
      if (response.mappings().isEmpty()) {
        throw new IndexNotFoundException(indexExpression[0]);
      }
      return response.mappings().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new IndexMapping(e.getValue())));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to get index mappings for "
              + String.join(",", indexExpression)
              + ": "
              + e.getMessage(),
          e);
    }
  }

  @Override
  public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {
    GetSettingsRequest request =
        new GetSettingsRequest().indices(indexExpression).includeDefaults(true);
    try {
      GetSettingsResponse response = client.indices().getSettings(request, RequestOptions.DEFAULT);
      Map<String, Settings> settings = response.getIndexToSettings();
      Map<String, Settings> defaultSettings = response.getIndexToDefaultSettings();
      Map<String, Integer> result = new HashMap<>();

      defaultSettings.forEach(
          (key, value) -> {
            Integer maxResultWindow = value.getAsInt("index.max_result_window", null);
            if (maxResultWindow != null) {
              result.put(key, maxResultWindow);
            }
          });

      settings.forEach(
          (key, value) -> {
            Integer maxResultWindow = value.getAsInt("index.max_result_window", null);
            if (maxResultWindow != null) {
              result.put(key, maxResultWindow);
            }
          });

      return result;
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to get max result window for "
              + String.join(",", indexExpression)
              + ": "
              + e.getMessage(),
          e);
    }
  }

  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    return request.search(
        req -> {
          try {
            // For RestClient with PIT: remove indices to avoid validation error
            // "indices cannot be used with point in time"
            if (req.source() != null && req.source().pointInTimeBuilder() != null) {
              req = new SearchRequest().source(req.source());
            }
            return client.search(req, RequestOptions.DEFAULT);
          } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to perform search operation with request " + req, e);
          }
        },
        req -> {
          try {
            return client.scroll(req, RequestOptions.DEFAULT);
          } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to perform scroll operation with request " + req, e);
          }
        });
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    try {
      GetIndexResponse indexResponse =
          client.indices().get(new GetIndexRequest(), RequestOptions.DEFAULT);
      final Stream<String> aliasStream =
          ImmutableList.copyOf(indexResponse.getAliases().values()).stream()
              .flatMap(Collection::stream)
              .map(AliasMetadata::alias);
      return Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get indices", e);
    }
  }

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  @Override
  public Map<String, String> meta() {
    try {
      final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
      ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
      request.includeDefaults(true);
      request.local(true);
      final Settings defaultSettings =
          client.cluster().getSettings(request, RequestOptions.DEFAULT).getDefaultSettings();
      builder.put(META_CLUSTER_NAME, defaultSettings.get("cluster.name", "opensearch"));
      builder.put(
          "plugins.sql.pagination.api", defaultSettings.get("plugins.sql.pagination.api", "true"));
      return builder.build();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get cluster meta info", e);
    }
  }

  @Override
  public void forceCleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.forceClean(
          scrollId -> {
            try {
              ClearScrollRequest clearRequest = new ClearScrollRequest();
              clearRequest.addScrollId(scrollId);
              client.clearScroll(clearRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
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
              ClearScrollRequest clearRequest = new ClearScrollRequest();
              clearRequest.addScrollId(scrollId);
              client.clearScroll(clearRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
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
    task.run();
  }

  @Override
  public Optional<NodeClient> getNodeClient() {
    return Optional.empty();
  }

  @Override
  public String createPit(CreatePitRequest createPitRequest) {
    try {
      CreatePitResponse createPitResponse =
          client.createPit(createPitRequest, RequestOptions.DEFAULT);
      return createPitResponse.getId();
    } catch (IOException e) {
      throw new RuntimeException("Error occurred while creating PIT for new engine SQL query", e);
    }
  }

  @Override
  public void deletePit(DeletePitRequest deletePitRequest) {
    try {
      DeletePitResponse deletePitResponse =
          client.deletePit(deletePitRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException(
          "Error occurred while deleting PIT for internal plugin operation", e);
    }
  }

  @Override
  public Map<String, Object> clusterHealth(Map<String, String> params) {
    try {
      ClusterHealthRequest request = new ClusterHealthRequest();
      if (params != null && Boolean.parseBoolean(params.get("local"))) {
        request.local(true);
      }
      ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
      return flattenHealth(response);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get cluster health", e);
    }
  }

  @Override
  public List<Map<String, Object>> catIndices(Map<String, String> params) {
    try {
      ClusterHealthResponse response =
          client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
      List<Map<String, Object>> rows = new ArrayList<>();
      for (Map.Entry<String, ClusterIndexHealth> entry : response.getIndices().entrySet()) {
        ClusterIndexHealth health = entry.getValue();
        Map<String, Object> row = new HashMap<>();
        row.put("index", entry.getKey());
        row.put("health", health.getStatus().name().toLowerCase(Locale.ROOT));
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
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get cat indices", e);
    }
  }

  @Override
  public List<Map<String, Object>> catNodes(Map<String, String> params) {
    List<Map<String, Object>> raw =
        catJson("/_cat/nodes", "name,ip,node.role,heap.percent,ram.percent,cpu");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map<String, Object> r : raw) {
      Map<String, Object> row = new HashMap<>();
      row.put("name", r.get("name"));
      row.put("ip", r.get("ip"));
      row.put("node_role", r.get("node.role"));
      row.put("heap_percent", asInt(r.get("heap.percent")));
      row.put("ram_percent", asInt(r.get("ram.percent")));
      row.put("cpu", asInt(r.get("cpu")));
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> catClusterManager(Map<String, String> params) {
    List<Map<String, Object>> raw = catJson("/_cat/cluster_manager", "id,host,ip,node");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map<String, Object> r : raw) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", r.get("id"));
      row.put("host", r.get("host"));
      row.put("ip", r.get("ip"));
      row.put("node", r.get("node"));
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> catPlugins(Map<String, String> params) {
    List<Map<String, Object>> raw = catJson("/_cat/plugins", "name,component,version");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map<String, Object> r : raw) {
      Map<String, Object> row = new HashMap<>();
      row.put("name", r.get("name"));
      row.put("component", r.get("component"));
      row.put("version", r.get("version"));
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> catShards(Map<String, String> params) {
    List<Map<String, Object>> raw = catJson("/_cat/shards", "index,shard,prirep,state,node");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map<String, Object> r : raw) {
      Map<String, Object> row = new HashMap<>();
      row.put("index", r.get("index"));
      row.put("shard", asInt(r.get("shard")));
      row.put("prirep", r.get("prirep"));
      row.put("state", r.get("state"));
      row.put("node", r.get("node"));
      rows.add(row);
    }
    return rows;
  }

  /** Standalone-mode helper: GET a _cat endpoint as JSON via the low-level client. */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> catJson(String path, String columns) {
    try {
      org.opensearch.client.Request request = new org.opensearch.client.Request("GET", path);
      request.addParameter("format", "json");
      request.addParameter("h", columns);
      org.opensearch.client.Response response = client.getLowLevelClient().performRequest(request);
      try (org.opensearch.core.xcontent.XContentParser parser =
          org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
              org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
              org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE,
              response.getEntity().getContent())) {
        List<Object> list = parser.list();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Object o : list) {
          rows.add((Map<String, Object>) o);
        }
        return rows;
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed GET " + path, e);
    }
  }

  private static Integer asInt(Object value) {
    if (value == null) {
      return null;
    }
    try {
      return (int) Double.parseDouble(value.toString().trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> clusterState(Map<String, String> params) {
    Map<String, Object> state =
        getJsonMap(
            "/_cluster/state/master_node,version,metadata,nodes",
            // nodes.*.name resolves the manager id to a name without over-fetching node IPs.
            Map.of(
                "filter_path",
                "cluster_name,state_uuid,version,cluster_manager_node,nodes.*.name"));
    Map<String, Object> row = new HashMap<>();
    row.put("cluster_name", state.get("cluster_name"));
    row.put("state_uuid", state.get("state_uuid"));
    row.put("version", asLong(state.get("version")));
    Object cmId = state.get("cluster_manager_node");
    String cmName = null;
    Object nodes = state.get("nodes");
    if (cmId != null && nodes instanceof Map) {
      Object n = ((Map<String, Object>) nodes).get(cmId.toString());
      if (n instanceof Map) {
        Object name = ((Map<String, Object>) n).get("name");
        cmName = name == null ? null : name.toString();
      }
    }
    row.put("cluster_manager_node", cmName);
    return row;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> clusterSettings(Map<String, String> params) {
    Map<String, Object> body = getJsonMap("/_cluster/settings", Map.of("flat_settings", "true"));
    List<Map<String, Object>> rows = new ArrayList<>();
    for (String tier : new String[] {"persistent", "transient"}) {
      Object section = body.get(tier);
      if (section instanceof Map) {
        for (Map.Entry<String, Object> e : ((Map<String, Object>) section).entrySet()) {
          Map<String, Object> row = new HashMap<>();
          row.put("setting", e.getKey());
          row.put("value", e.getValue() == null ? null : e.getValue().toString());
          row.put("tier", tier);
          rows.add(row);
        }
      }
    }
    return rows;
  }

  /** Standalone-mode helper: GET a JSON-object endpoint via the low-level client. */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getJsonMap(String path, Map<String, String> params) {
    try {
      org.opensearch.client.Request request = new org.opensearch.client.Request("GET", path);
      if (params != null) {
        params.forEach(request::addParameter);
      }
      org.opensearch.client.Response response = client.getLowLevelClient().performRequest(request);
      try (org.opensearch.core.xcontent.XContentParser parser =
          org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
              org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
              org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE,
              response.getEntity().getContent())) {
        return parser.map();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed GET " + path, e);
    }
  }

  private static Long asLong(Object value) {
    if (value == null) {
      return null;
    }
    try {
      return (long) Double.parseDouble(value.toString().trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> resolveIndex(Map<String, String> params) {
    String expandWildcards = params == null ? null : params.get("expand_wildcards");
    Map<String, Object> body =
        getJsonMap(
            "/_resolve/index/*",
            expandWildcards == null ? Map.of() : Map.of("expand_wildcards", expandWildcards));
    List<Map<String, Object>> rows = new ArrayList<>();
    addResolved(body.get("indices"), "index", rows);
    addResolved(body.get("aliases"), "alias", rows);
    addResolved(body.get("data_streams"), "data_stream", rows);
    return rows;
  }

  @SuppressWarnings("unchecked")
  private void addResolved(Object section, String type, List<Map<String, Object>> rows) {
    if (section instanceof List) {
      for (Object o : (List<Object>) section) {
        if (o instanceof Map) {
          Map<String, Object> row = new HashMap<>();
          row.put("name", ((Map<String, Object>) o).get("name"));
          row.put("type", type);
          rows.add(row);
        }
      }
    }
  }

  private Map<String, Object> flattenHealth(ClusterHealthResponse response) {
    Map<String, Object> row = new HashMap<>();
    row.put("cluster_name", response.getClusterName());
    row.put("status", response.getStatus().name().toLowerCase(Locale.ROOT));
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
