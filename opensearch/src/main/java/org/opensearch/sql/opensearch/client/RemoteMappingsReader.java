/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Reads remote index mappings and settings with each remote cluster's own GetMappings and
 * GetSettings, through a per-cluster client. Both read the remote gateway node's cluster state
 * ({@code local=true}), so no shard is touched.
 *
 * <p>Each remote cluster is asked on its own, so one that fails reports its own error: it is
 * skipped when its {@code skip_unavailable} is true, and fails the query otherwise.
 */
class RemoteMappingsReader {

  /** Mappings and max result windows of the indices named, keyed by "cluster:index". */
  record RemoteMetadata(Map<String, IndexMapping> mappings, Map<String, Integer> windows) {}

  private static final long TTL_SECONDS = 60;

  /** Most top-level fields kept across all cached entries. */
  private static final long MAX_TOTAL_FIELDS = 100000;

  private static final Cache<List<Object>, RemoteMetadata> CACHE =
      CacheBuilder.newBuilder()
          // One segment: Guava splits maximumWeight across segments and drops any entry heavier
          // than one segment's share right after loading it.
          .concurrencyLevel(1)
          .maximumWeight(MAX_TOTAL_FIELDS)
          .weigher((List<Object> key, RemoteMetadata metadata) -> weigh(metadata))
          .expireAfterWrite(TTL_SECONDS, TimeUnit.SECONDS)
          .build();

  private final NodeClient client;
  private final RemoteClusterService remotes;
  private final TimeValue timeout;

  RemoteMappingsReader(NodeClient client, RemoteClusterService remotes, TimeValue timeout) {
    this.client = client;
    this.remotes = remotes;
    this.timeout = timeout;
  }

  /**
   * Cached per user (field-level security) and indices; an answer with a skipped cluster is never
   * cached.
   *
   * @param user security user of the request, or null when security is off
   * @param indicesByCluster remote cluster alias to the index names asked of it
   */
  RemoteMetadata read(Object user, Map<String, String[]> indicesByCluster) {
    List<Object> key = List.of(Objects.toString(user, ""), qualifiedNames(indicesByCluster));
    try {
      return CACHE.get(key, () -> fetch(indicesByCluster));
    } catch (UncheckedExecutionException | ExecutionException e) {
      if (e.getCause() instanceof NotCached notCached) {
        return notCached.metadata;
      }
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(e.getCause());
    }
  }

  private RemoteMetadata fetch(Map<String, String[]> indicesByCluster) {
    String indexNames = String.join(",", qualifiedNames(indicesByCluster));
    Map<String, IndexMapping> mappings = new LinkedHashMap<>();
    Map<String, Integer> windows = new LinkedHashMap<>();
    // Rollover indices usually share one mapping: parse it once and share the result, so cost and
    // memory follow the number of distinct mappings rather than the number of indices.
    Map<MappingMetadata, IndexMapping> parsed = new HashMap<>();
    List<String> skipped = new ArrayList<>();
    indicesByCluster.forEach(
        (clusterAlias, indices) -> {
          try {
            Client remote = client.getRemoteClusterClient(clusterAlias);
            GetMappingsResponse mappingsResponse =
                remote.admin().indices().prepareGetMappings(indices).setLocal(true).get(timeout);
            GetSettingsResponse settingsResponse =
                remote
                    .admin()
                    .indices()
                    .getSettings(
                        new GetSettingsRequest().indices(indices).local(true).includeDefaults(true))
                    .actionGet(timeout);
            mappingsResponse
                .mappings()
                .forEach(
                    (index, mapping) ->
                        mappings.put(
                            RemoteClusterAware.buildRemoteIndexName(clusterAlias, index),
                            parsed.computeIfAbsent(mapping, IndexMapping::new)));
            settingsResponse
                .getIndexToSettings()
                .forEach(
                    (index, settings) ->
                        windows.put(
                            RemoteClusterAware.buildRemoteIndexName(clusterAlias, index),
                            settings.getAsInt(
                                IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                                IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(
                                    Settings.EMPTY))));
          } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            // A missing index or a denied read is the remote's answer, not an outage.
            if (cause instanceof IndexNotFoundException notFound) {
              throw IndexMappingErrors.indexNotFound(notFound, indexNames);
            }
            if (cause instanceof OpenSearchSecurityException denied) {
              throw IndexMappingErrors.permissionDenied(denied, indexNames);
            }
            if (!remotes.isSkipUnavailable(clusterAlias)) {
              throw e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e);
            }
            skipped.add(clusterAlias);
          }
        });

    if (mappings.isEmpty()) {
      if (!skipped.isEmpty()) {
        throw new AllClustersSkipped(skippedMessage(skipped), indexNames);
      }
      throw IndexMappingErrors.indexNotFound(new IndexNotFoundException(indexNames), indexNames);
    }
    if (!skipped.isEmpty()) {
      // Partial answer during an outage: serve it, but don't cache it. The search that follows
      // reports the skipped cluster.
      throw new NotCached(new RemoteMetadata(mappings, windows));
    }
    return new RemoteMetadata(mappings, windows);
  }

  /** Clusters named in a skipped-cluster message; the rest are counted. */
  private static final int MAX_NAMED_CLUSTERS = 5;

  /** Names up to {@link #MAX_NAMED_CLUSTERS} skipped clusters, sorted, and counts the rest. */
  static String skippedMessage(List<String> clusters) {
    List<String> sorted = clusters.stream().sorted().toList();
    String named =
        sorted.stream().limit(MAX_NAMED_CLUSTERS).collect(Collectors.joining(", ", "[", "]"));
    int more = sorted.size() - MAX_NAMED_CLUSTERS;
    String message =
        sorted.size() == 1
            ? "Remote cluster " + named + " is unavailable and was skipped"
            : "Remote clusters "
                + named
                + (more > 0 ? " and " + more + " more" : "")
                + " are unavailable and were skipped";
    return message + " (skip_unavailable is true)";
  }

  /**
   * Every remote cluster asked was unreachable and skipped, so there are no remote fields. The
   * caller decides: local indices in the same query still answer, and otherwise it is an error.
   */
  static final class AllClustersSkipped extends RuntimeException {
    /** The "cluster:index" names asked, for the error's context. */
    final String indexNames;

    AllClustersSkipped(String message, String indexNames) {
      super(message, null, false, false);
      this.indexNames = indexNames;
    }
  }

  /** Carries a partial answer out of the cache loader without caching it. */
  static final class NotCached extends RuntimeException {
    final transient RemoteMetadata metadata;

    NotCached(RemoteMetadata metadata) {
      super(null, null, false, false);
      this.metadata = metadata;
    }
  }

  /** Field count of the distinct mappings: indices sharing one mapping weigh it once. */
  static int weigh(RemoteMetadata metadata) {
    Set<IndexMapping> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
    distinct.addAll(metadata.mappings().values());
    return Math.max(1, distinct.stream().mapToInt(m -> m.getFieldMappings().size()).sum());
  }

  static void clear() {
    CACHE.invalidateAll();
  }

  /** "cluster:index" for every index asked of every cluster, sorted, as the cache key. */
  private static List<String> qualifiedNames(Map<String, String[]> indicesByCluster) {
    return indicesByCluster.entrySet().stream()
        .flatMap(
            entry ->
                Arrays.stream(entry.getValue())
                    .map(index -> RemoteClusterAware.buildRemoteIndexName(entry.getKey(), index)))
        .sorted()
        .toList();
  }
}
