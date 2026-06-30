/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.transport.client.node.NodeClient;

/**
 * OpenSearch client abstraction to wrap different OpenSearch client implementation. For example,
 * implementation by node client for OpenSearch plugin or by REST client for standalone mode.
 */
public interface OpenSearchClient {

  String META_CLUSTER_NAME = "CLUSTER_NAME";

  /**
   * Check if the given index exists.
   *
   * @param indexName index name
   * @return true if exists, otherwise false
   */
  boolean exists(String indexName);

  /**
   * Create OpenSearch index based on the given mappings.
   *
   * @param indexName index name
   * @param mappings index mappings
   */
  void createIndex(String indexName, Map<String, Object> mappings);

  /**
   * Fetch index mapping(s) according to index expression given.
   *
   * @param indexExpression index expression
   * @return index mapping(s) from index name to its mapping
   */
  Map<String, IndexMapping> getIndexMappings(String... indexExpression);

  /**
   * Fetch index.max_result_window settings according to index expression given.
   *
   * @param indexExpression index expression
   * @return map from index name to its max result window
   */
  Map<String, Integer> getIndexMaxResultWindows(String... indexExpression);

  /**
   * Perform search query in the search request.
   *
   * @param request search request
   * @return search response
   */
  OpenSearchResponse search(OpenSearchRequest request);

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  List<String> indices();

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  Map<String, String> meta();

  /**
   * Force to clean up resources related to the search request.
   *
   * @param request search request
   */
  void forceCleanup(OpenSearchRequest request);

  /**
   * Clean up resources related to the search request, for example scroll context.
   *
   * @param request search request
   */
  void cleanup(OpenSearchRequest request);

  /**
   * Schedule a task to run.
   *
   * @param task task
   */
  void schedule(Runnable task);

  Optional<NodeClient> getNodeClient();

  /**
   * Create PIT for given indices
   *
   * @param createPitRequest Create Point In Time request
   * @return PitId
   */
  String createPit(CreatePitRequest createPitRequest);

  /**
   * Delete PIT
   *
   * @param deletePitRequest Delete Point In Time request
   */
  void deletePit(DeletePitRequest deletePitRequest);

  /**
   * Read-only cluster health snapshot for the {@code rest} command (backs {@code
   * /_cluster/health}). Returns a single flattened row of health fields. Runs under the caller's
   * security thread-context; performs no privilege escalation and mutates nothing.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return a single map of health field name to value
   */
  default Map<String, Object> clusterHealth(Map<String, String> params) {
    throw new UnsupportedOperationException("clusterHealth is not supported by this client");
  }

  /**
   * Read-only cat-indices listing for the {@code rest} command (backs {@code /_cat/indices}). One
   * map per index. Runs under the caller's security thread-context; read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return one map of column name to value per index
   */
  default List<Map<String, Object>> catIndices(Map<String, String> params) {
    throw new UnsupportedOperationException("catIndices is not supported by this client");
  }

  /**
   * Read-only cat-nodes listing for the {@code rest} command (backs {@code /_cat/nodes}). One map
   * per node with resource state. Runs under the caller's security thread-context; read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return one map of column name to value per node
   */
  default List<Map<String, Object>> catNodes(Map<String, String> params) {
    throw new UnsupportedOperationException("catNodes is not supported by this client");
  }

  /**
   * Read-only cat-cluster_manager listing for the {@code rest} command (backs {@code
   * /_cat/cluster_manager}). Single map identifying the elected cluster manager. Read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return one map describing the cluster manager node
   */
  default List<Map<String, Object>> catClusterManager(Map<String, String> params) {
    throw new UnsupportedOperationException("catClusterManager is not supported by this client");
  }

  /**
   * Read-only cat-plugins listing for the {@code rest} command (backs {@code /_cat/plugins}). One
   * map per installed plugin per node. Read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return one map of column name to value per plugin
   */
  default List<Map<String, Object>> catPlugins(Map<String, String> params) {
    throw new UnsupportedOperationException("catPlugins is not supported by this client");
  }

  /**
   * Read-only cat-shards listing for the {@code rest} command (backs {@code /_cat/shards}). One map
   * per shard. Read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return one map of column name to value per shard
   */
  default List<Map<String, Object>> catShards(Map<String, String> params) {
    throw new UnsupportedOperationException("catShards is not supported by this client");
  }

  /**
   * Read-only cluster-state epoch projection for the {@code rest} command (backs {@code
   * /_cluster/state}). Single flattened row (cluster_name, state_uuid, version,
   * cluster_manager_node). Read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return a single map of cluster-state field name to value
   */
  default Map<String, Object> clusterState(Map<String, String> params) {
    throw new UnsupportedOperationException("clusterState is not supported by this client");
  }

  /**
   * Read-only cluster-settings listing for the {@code rest} command (backs {@code
   * /_cluster/settings}). One map per configured setting (setting, value, tier). Read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return one map of column name to value per setting
   */
  default List<Map<String, Object>> clusterSettings(Map<String, String> params) {
    throw new UnsupportedOperationException("clusterSettings is not supported by this client");
  }

  /**
   * Read-only resolve-index listing for the {@code rest} command (backs {@code /_resolve/index}).
   * One map per resolved index, alias, or data stream (name, type). Read-only.
   *
   * @param params endpoint query args (already allow-list-validated)
   * @return one map of column name to value per resolved name
   */
  default List<Map<String, Object>> resolveIndex(Map<String, String> params) {
    throw new UnsupportedOperationException("resolveIndex is not supported by this client");
  }
}
