/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.flint.core.storage.FlintReader;

/**
 * Flint index client that provides API for metadata and data operations
 * on a Flint index regardless of concrete storage.
 */
public interface FlintClient {

  /**
   * Create a Flint index with the metadata given.
   *
   * @param indexName index name
   * @param metadata  index metadata
   */
  void createIndex(String indexName, FlintMetadata metadata);

  /**
   * Does Flint index with the given name exist
   *
   * @param indexName index name
   * @return true if the index exists, otherwise false
   */
  boolean exists(String indexName);

  /**
   * Retrieve metadata in a Flint index.
   *
   * @param indexName index name
   * @return index metadata
   */
  FlintMetadata getIndexMetadata(String indexName);

  /**
   * Delete a Flint index.
   *
   * @param indexName index name
   */
  void deleteIndex(String indexName);

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all
   * @return {@link FlintReader}.
   */
  FlintReader createReader(String indexName, String query);
}
