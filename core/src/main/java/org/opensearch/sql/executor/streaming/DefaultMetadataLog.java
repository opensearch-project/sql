/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.streaming;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;

/**
 * In memory implementation of {@link MetadataLog}. Todo. Current implementation does not guarantee
 * thread safe. We will re-evaluate it when adding pipeline execution.
 *
 * @param <T> type of metadata type.
 */
public class DefaultMetadataLog<T> implements MetadataLog<T> {

  private static final long MIN_ACCEPTABLE_ID = 0L;

  private SortedMap<Long, T> metadataMap = new TreeMap<>();

  @Override
  public boolean add(Long batchId, T metadata) {
    Preconditions.checkArgument(batchId >= MIN_ACCEPTABLE_ID, "batch id must large or equal 0");

    if (metadataMap.containsKey(batchId)) {
      return false;
    }
    metadataMap.put(batchId, metadata);
    return true;
  }

  @Override
  public Optional<T> get(Long batchId) {
    if (!metadataMap.containsKey(batchId)) {
      return Optional.empty();
    } else {
      return Optional.of(metadataMap.get(batchId));
    }
  }

  @Override
  public List<T> get(Optional<Long> startBatchId, Optional<Long> endBatchId) {
    if (startBatchId.isEmpty() && endBatchId.isEmpty()) {
      return new ArrayList<>(metadataMap.values());
    } else {
      Long s = startBatchId.orElse(MIN_ACCEPTABLE_ID);
      Long e = endBatchId.map(i -> i + 1).orElse(Long.MAX_VALUE);
      return new ArrayList<>(metadataMap.subMap(s, e).values());
    }
  }

  @Override
  public Optional<Pair<Long, T>> getLatest() {
    if (metadataMap.isEmpty()) {
      return Optional.empty();
    } else {
      Long latestId = metadataMap.lastKey();
      return Optional.of(Pair.of(latestId, metadataMap.get(latestId)));
    }
  }

  @Override
  public void purge(Long batchId) {
    metadataMap.headMap(batchId).clear();
  }
}
