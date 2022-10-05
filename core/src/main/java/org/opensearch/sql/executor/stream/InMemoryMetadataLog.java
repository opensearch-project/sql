/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;

// for test only
public class InMemoryMetadataLog<T> implements MetadataLog<T> {
  private SortedMap<Long, T> map = new TreeMap<>();

  @Override
  public boolean add(Long batchId, T metadata) {
    if (map.containsKey(batchId)) {
      return false;
    }
    map.put(batchId, metadata);
    return true;
  }

  @Override
  public Optional<T> get(Long batchId) {
    if (!map.containsKey(batchId)) {
      return Optional.empty();
    } else {
      return Optional.of(map.get(batchId));
    }
  }

  @Override
  public List<T> get(Optional<Long> start, Optional<Long> end) {
    if (start.isEmpty() && end.isEmpty()) {
      return new ArrayList<>(map.values());
    } else {
      Long s = start.orElse(-1L);
      Long e = end.map(i -> i + 1).orElse(Long.MAX_VALUE);
      return new ArrayList<>(map.subMap(s, e).values());
    }
  }

  @Override
  public Optional<Pair<Long, T>> getLatest() {
    if (map.isEmpty()) {
      return Optional.empty();
    } else {
      final Long latest = map.lastKey();
      return Optional.of(Pair.of(latest, map.get(latest)));
    }
  }

  @Override
  public void purge(Long batchId) {
    for(long i = -1L; i < batchId; i++) {
      map.remove(i);
    }
  }
}
