/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

class RemoteMappingsReaderTest {

  private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(10);

  private final NodeClient client = mock(NodeClient.class);
  private final RemoteClusterService remotes = mock(RemoteClusterService.class);
  private final Map<String, Client> remoteClients = new LinkedHashMap<>();

  @BeforeEach
  void setUp() {
    RemoteMappingsReader.clear();
  }

  @Test
  void readsEachClusterAndPrefixesItsIndexNames() {
    givenRemote("a", Map.of("logs-1", 10000));
    givenRemote("b", Map.of("logs-2", 10000));

    RemoteMappingsReader.RemoteMetadata metadata = read("alice", "a:logs-*", "b:logs-*");

    assertEquals(java.util.Set.of("a:logs-1", "b:logs-2"), metadata.mappings().keySet());
    assertEquals(
        "keyword",
        metadata
            .mappings()
            .get("a:logs-1")
            .getFieldMappings()
            .get("f")
            .legacyTypeName()
            .toLowerCase(java.util.Locale.ROOT));
  }

  /** Rollover indices share a mapping: it is parsed once and weighed once in the cache. */
  @Test
  void indicesWithTheSameMappingShareOneParsedCopy() {
    givenRemote("a", Map.of("logs-1", 10000, "logs-2", 10000, "logs-3", 10000));

    RemoteMappingsReader.RemoteMetadata metadata = read("alice", "a:logs-*");

    assertSame(metadata.mappings().get("a:logs-1"), metadata.mappings().get("a:logs-2"));
    assertSame(metadata.mappings().get("a:logs-1"), metadata.mappings().get("a:logs-3"));
    assertEquals(1, RemoteMappingsReader.weigh(metadata));
  }

  /** Below the cache's total limit, a schema of many distinct mappings is still cached. */
  @Test
  void largeSchemaWithManyDistinctMappingsIsCached() {
    int indices = 30;
    int fieldsEach = 1000;
    Map<String, MappingMetadata> mappings = new LinkedHashMap<>();
    Map<String, Settings> settings = new LinkedHashMap<>();
    for (int i = 0; i < indices; i++) {
      Map<String, Object> properties = new LinkedHashMap<>();
      for (int f = 0; f < fieldsEach; f++) {
        properties.put("i" + i + "_f" + f, Map.of("type", "keyword"));
      }
      mappings.put("logs-" + i, new MappingMetadata("_doc", Map.of("properties", properties)));
      settings.put("logs-" + i, Settings.EMPTY);
    }
    Client remote = remoteClient("a");
    when(remote
            .admin()
            .indices()
            .prepareGetMappings(any(String[].class))
            .setLocal(true)
            .get(TIMEOUT))
        .thenReturn(new GetMappingsResponse(mappings));
    when(remote.admin().indices().getSettings(any()).actionGet(TIMEOUT))
        .thenReturn(new GetSettingsResponse(settings, Map.of()));

    read("alice", "a:logs-*");
    read("alice", "a:logs-*");

    verify(client, times(1)).getRemoteClusterClient("a");
  }

  @Test
  void readsTheRemoteMaxResultWindow() {
    givenRemote("a", Map.of("logs-1", 50000));

    assertEquals(50000, read("alice", "a:logs-1").windows().get("a:logs-1"));
  }

  @Test
  void secondReadBySameUserIsCached() {
    givenRemote("a", Map.of("logs-1", 10000));

    RemoteMappingsReader.RemoteMetadata first = read("alice", "a:logs-1");
    RemoteMappingsReader.RemoteMetadata second = read("alice", "a:logs-1");

    assertSame(first, second);
    verify(client, times(1)).getRemoteClusterClient("a");
  }

  /** Field-level security is per user, so users never share an entry. */
  @Test
  void usersDoNotShareEntries() {
    givenRemote("a", Map.of("logs-1", 10000));

    read("alice", "a:logs-1");
    read("bob", "a:logs-1");

    verify(client, times(2)).getRemoteClusterClient("a");
  }

  @Test
  void skipsAnUnreachableClusterWhenSkipUnavailable() {
    givenRemote("a", Map.of("logs-1", 10000));
    givenUnreachable("b", true);

    RemoteMappingsReader.RemoteMetadata metadata = read("alice", "a:logs-*", "b:logs-*");

    assertEquals(java.util.Set.of("a:logs-1"), metadata.mappings().keySet());
  }

  /** An answer missing a skipped cluster is an outage answer, so it is never cached. */
  @Test
  void partialAnswerIsNotCached() {
    givenRemote("a", Map.of("logs-1", 10000));
    givenUnreachable("b", true);

    read("alice", "a:logs-*", "b:logs-*");
    read("alice", "a:logs-*", "b:logs-*");

    verify(client, times(2)).getRemoteClusterClient("a");
  }

  @Test
  void failsOnAnUnreachableClusterWithoutSkipUnavailable() {
    givenRemote("a", Map.of("logs-1", 10000));
    givenUnreachable("b", false);

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> read("alice", "a:logs-*", "b:logs-*"));
    assertTrue(e.getMessage().contains("Unable to open any proxy connections"));
  }

  /** Every cluster skipped: no remote fields, signalled for the caller to decide. */
  @Test
  void everyClusterSkippedIsSignalledWithItsMessage() {
    givenUnreachable("b", true);

    RemoteMappingsReader.AllClustersSkipped e =
        assertThrows(
            RemoteMappingsReader.AllClustersSkipped.class, () -> read("alice", "b:logs-*"));
    assertEquals(
        "Remote cluster [b] is unavailable and was skipped (skip_unavailable is true)",
        e.getMessage());
  }

  @Test
  void messageNamesSeveralSkippedClustersInOrder() {
    assertEquals(
        "Remote clusters [a, b, c] are unavailable and were skipped (skip_unavailable is true)",
        RemoteMappingsReader.skippedMessage(java.util.List.of("c", "a", "b")));
  }

  /** Many skipped clusters: the message names five and counts the rest. */
  @Test
  void messageNamesFiveClustersAndCountsTheRest() {
    assertEquals(
        "Remote clusters [a, b, c, d, e] and 2 more are unavailable and were skipped"
            + " (skip_unavailable is true)",
        RemoteMappingsReader.skippedMessage(java.util.List.of("g", "f", "e", "d", "c", "b", "a")));
  }

  /** A missing index is the remote's answer, not an outage: skip_unavailable does not hide it. */
  @Test
  void missingIndexIsReportedEvenWithSkipUnavailable() {
    givenFailure("a", new IndexNotFoundException("nope"));
    when(remotes.isSkipUnavailable("a")).thenReturn(true);

    ErrorReport report = assertThrows(ErrorReport.class, () -> read("alice", "a:nope"));
    assertEquals(ErrorCode.INDEX_NOT_FOUND, report.getCode());
  }

  @Test
  void deniedReadIsAPermissionError() {
    givenFailure("a", new OpenSearchSecurityException("no permissions", RestStatus.FORBIDDEN));

    ErrorReport report = assertThrows(ErrorReport.class, () -> read("alice", "a:logs-1"));
    assertEquals(ErrorCode.PERMISSION_DENIED, report.getCode());
  }

  @Test
  void patternMatchingNothingIsIndexNotFound() {
    givenRemote("a", Map.of());

    ErrorReport report = assertThrows(ErrorReport.class, () -> read("alice", "a:none-*"));
    assertEquals(ErrorCode.INDEX_NOT_FOUND, report.getCode());
  }

  /** Reads "cluster:index" names, grouped by cluster as the node client passes them. */
  private RemoteMappingsReader.RemoteMetadata read(String user, String... names) {
    Map<String, java.util.List<String>> byCluster = new LinkedHashMap<>();
    for (String name : names) {
      int separator = name.indexOf(':');
      byCluster
          .computeIfAbsent(name.substring(0, separator), k -> new java.util.ArrayList<>())
          .add(name.substring(separator + 1));
    }
    Map<String, String[]> indicesByCluster = new LinkedHashMap<>();
    byCluster.forEach(
        (cluster, indices) -> indicesByCluster.put(cluster, indices.toArray(String[]::new)));
    return new RemoteMappingsReader(client, remotes, TIMEOUT).read(user, indicesByCluster);
  }

  /** A reachable cluster whose indices each have one keyword field "f". */
  private void givenRemote(String alias, Map<String, Integer> windowsByIndex) {
    Map<String, MappingMetadata> mappings = new LinkedHashMap<>();
    Map<String, Settings> settings = new LinkedHashMap<>();
    windowsByIndex.forEach(
        (index, window) -> {
          mappings.put(
              index,
              new MappingMetadata(
                  "_doc", Map.of("properties", Map.of("f", Map.of("type", "keyword")))));
          settings.put(index, Settings.builder().put("index.max_result_window", window).build());
        });
    Client remote = remoteClient(alias);
    when(remote
            .admin()
            .indices()
            .prepareGetMappings(any(String[].class))
            .setLocal(true)
            .get(TIMEOUT))
        .thenReturn(new GetMappingsResponse(mappings));
    when(remote.admin().indices().getSettings(any()).actionGet(TIMEOUT))
        .thenReturn(new GetSettingsResponse(settings, Map.of()));
  }

  private void givenUnreachable(String alias, boolean skipUnavailable) {
    givenFailure(
        alias,
        new IllegalStateException(
            "Unable to open any proxy connections to remote cluster [" + alias + "]"));
    when(remotes.isSkipUnavailable(alias)).thenReturn(skipUnavailable);
  }

  private void givenFailure(String alias, RuntimeException failure) {
    Client remote = remoteClient(alias);
    when(remote
            .admin()
            .indices()
            .prepareGetMappings(any(String[].class))
            .setLocal(true)
            .get(TIMEOUT))
        .thenThrow(failure);
  }

  private Client remoteClient(String alias) {
    return remoteClients.computeIfAbsent(
        alias,
        a -> {
          Client remote = mock(Client.class, RETURNS_DEEP_STUBS);
          when(client.getRemoteClusterClient(a)).thenReturn(remote);
          return remote;
        });
  }
}
