/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stream;

import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

public interface MetadataLog<T> {

  boolean add(Long batchId, T metadata);

  Optional<T> get(Long batchId);

  /**
   * Return metadata for batches between startId (inclusive) and endId (inclusive). If `startId` is
   * `None`, just return all batches before endId (inclusive).
   */
  List<T> get(Optional<Long> start, Optional<Long> end);

  /**
   * Get latest batchId and metadata.
   * @return pair of batchId and metadata if not empty.
   */
  Optional<Pair<Long, T>> getLatest();

  /**
   * Remove all the metadata less then batchId.
   * @param batchId smallest batchId should keep.
   */
  void purge(Long batchId);
}
