/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.flint.core.metadata.FlintMetadata;

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
   * Retrieve metadata in a Flint index.
   *
   * @param indexName index name
   * @return index metadata
   */
  FlintMetadata getIndexMetadata(String indexName);
}
