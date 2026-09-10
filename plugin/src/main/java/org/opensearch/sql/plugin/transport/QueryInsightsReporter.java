/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.EmptyTransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Serializes a completed PPL/SQL query record and sends it to Query Insights over the transport
 * layer. Uses the core {@link BytesTransportRequest} (loaded by both plugin classloaders, unlike a
 * plugin-defined type) sent via {@link TransportService#sendRequest}.
 */
public final class QueryInsightsReporter {

  private static final Logger LOG = LogManager.getLogger(QueryInsightsReporter.class);

  public static final String ACTION_NAME =
      "cluster:admin/opensearch/query_insights/report_query_bytes";

  public static final int FORMAT_VERSION = 2;

  private QueryInsightsReporter() {}

  /**
   * Serialize the record and send it to the local node's Query Insights handler; errors are ignored
   * (e.g. Query Insights not installed).
   *
   * @param parentMarker {@code <source>:<nodeId>:<taskId>}; child DSL records reference it via
   *     {@code DERIVED_FROM} so their cpu/memory roll up into this record
   * @param userInfo security {@code _opendistro_security_user_info} string; empty when unsecured
   */
  public static void report(
      TransportService transportService,
      DiscoveryNode localNode,
      String querySource,
      String parentMarker,
      String nodeId,
      String queryText,
      long timestampMillis,
      long latencyMillis,
      long cpuNanos,
      long memoryBytes,
      List<String> indices,
      String userInfo) {
    try {
      final BytesStreamOutput out = new BytesStreamOutput();
      out.writeVInt(FORMAT_VERSION);
      out.writeString(querySource == null ? "" : querySource);
      out.writeString(parentMarker == null ? "" : parentMarker);
      out.writeString(nodeId == null ? "" : nodeId);
      out.writeString(queryText == null ? "" : queryText);
      out.writeVLong(timestampMillis);
      out.writeVLong(Math.max(0L, latencyMillis));
      out.writeVLong(Math.max(0L, cpuNanos));
      out.writeVLong(Math.max(0L, memoryBytes));

      final List<String> safeIndices = indices == null ? List.of() : indices;
      out.writeVInt(safeIndices.size());
      for (final String index : safeIndices) {
        out.writeString(index == null ? "" : index);
      }

      out.writeString(userInfo == null ? "" : userInfo); // v2

      final BytesTransportRequest request = new BytesTransportRequest(out.bytes(), Version.CURRENT);
      // Stash the context so this internal cluster:admin send runs user-less (security's
      // Origin.LOCAL path) and isn't authorized against the end user. The handler reads the user
      // off the wire, so it needs no user context.
      final ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
      try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
        transportService.sendRequest(
            localNode, ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
      }
    } catch (Exception e) {
      LOG.debug("Failed to report PPL query to Query Insights", e);
    }
  }
}
