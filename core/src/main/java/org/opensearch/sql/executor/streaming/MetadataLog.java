/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.streaming;

import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Write-ahead Log (WAL). Which allow client write metadata associate with id.
 *
 * @param <T> type of metadata type.
 */
public interface MetadataLog<T> {

  /**
   * add metadata to WAL.
   *
   * @param id metadata index in WAL.
   * @param metadata metadata.
   * @return true if add success, otherwise return false.
   */
  boolean add(Long id, T metadata);

  /**
   * get metadata from WAL.
   *
   * @param id metadata index in WAL.
   * @return metadata.
   */
  Optional<T> get(Long id);

  /**
   * Return metadata for id between [startId, endId].
   *
   * @param startId If startId is empty, return all metadata before endId (inclusive).
   * @param endId If end is empty, return all batches after endId (inclusive).
   * @return a list of metadata sorted by id (nature order).
   */
  List<T> get(Optional<Long> startId, Optional<Long> endId);

  /**
   * Get latest batchId and metadata.
   *
   * @return pair of id and metadata if not empty.
   */
  Optional<Pair<Long, T>> getLatest();

  /**
   * Remove all the metadata less then id (exclusive).
   *
   * @param id smallest batchId should keep.
   */
  void purge(Long id);
}
