/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Map;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;

/** Interface for FlintIndexMetadataReader */
public interface FlintIndexMetadataService {

  /**
   * Retrieves a map of {@link FlintIndexMetadata} instances matching the specified index pattern.
   *
   * @param indexPattern indexPattern.
   * @return A map of {@link FlintIndexMetadata} instances against indexName, each providing
   *     metadata access for a matched index. Returns an empty list if no indices match the pattern.
   */
  Map<String, FlintIndexMetadata> getFlintIndexMetadata(String indexPattern);

  /**
   * Performs validation and updates flint index to manual refresh.
   *
   * @param indexName indexName.
   * @param flintIndexOptions flintIndexOptions.
   */
  void updateIndexToManualRefresh(String indexName, FlintIndexOptions flintIndexOptions);
}
