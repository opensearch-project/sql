/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import java.util.List;
import java.util.Map;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

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

  NodeClient getNodeClient();

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
}
