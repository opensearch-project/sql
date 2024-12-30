/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

/** Interface to abstract access to the FlintIndex */
public interface FlintIndexClient {
  void deleteIndex(String indexName);
}
