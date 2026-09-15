/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.EmptyTransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Serializes a completed PPL/SQL query record and sends it to Query Insights over the transport
 * layer, feeding its {@code addRecord} pipeline (Top N, historical index, roll-up).
 *
 * <p>Sent as the core {@link BytesTransportRequest} via {@link TransportService#sendRequest} (not
 * {@code client.execute}): the core request type is loaded by both plugin classloaders, avoiding
 * the {@link ClassCastException} a plugin-defined type would hit on same-node delivery.
 *
 * <p>The action name and wire format are owned by Query Insights and duplicated here as literals
 * (SQL must not depend on the Query Insights artifact), so they MUST stay in lock-step with {@code
 * ReportQueryBytesAction}.
 */
public final class QueryInsightsReporter {

  private static final Logger LOG = LogManager.getLogger(QueryInsightsReporter.class);

  /** Transport action name. MUST match {@code ReportQueryBytesAction.NAME} in Query Insights. */
  public static final String ACTION_NAME =
      "cluster:admin/opensearch/query_insights/report_query_bytes";

  /**
   * Wire format version. MUST match {@code ReportQueryBytesAction.FORMAT_VERSION} in Query
   * Insights. The two plugins are unreleased and always built together, so there is a single
   * format: any change to the layout below is a coordinated change on both sides, not a
   * compatibility boundary.
   */
  public static final int FORMAT_VERSION = 1;

  private QueryInsightsReporter() {}

  /**
   * Serialize the record and send it to the local node's Query Insights handler. Errors (e.g. Query
   * Insights not installed, so the action is unregistered) are logged at debug and ignored.
   *
   * @param transportService the transport service used to send the request
   * @param localNode the local (coordinator) node — the handler is registered on every node, so we
   *     send to ourselves
   * @param querySource the query source label (e.g. {@code "PPL"})
   * @param parentMarker the originating query's marker {@code <source>:<nodeId>:<taskId>}; used as
   *     both the record id and the parent marker so child DSL records (tagged with the same value
   *     via {@code DERIVED_FROM}) roll up into this record
   * @param nodeId the coordinator node id
   * @param queryText the (prefix-stripped) query text
   * @param timestampMillis the record timestamp
   * @param latencyMillis end-to-end coordinator latency
   * @param cpuNanos coordinator CPU nanos
   * @param memoryBytes coordinator memory bytes
   * @param indices the resolved index name(s) the query reads from; may be empty
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
      java.util.List<String> indices) {
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

      final java.util.List<String> safeIndices = indices == null ? java.util.List.of() : indices;
      out.writeVInt(safeIndices.size());
      for (final String index : safeIndices) {
        out.writeString(index == null ? "" : index);
      }

      final BytesTransportRequest request = new BytesTransportRequest(out.bytes(), Version.CURRENT);
      transportService.sendRequest(
          localNode, ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
    } catch (Exception e) {
      // Query Insights may not be installed (action unregistered) or the send may fail; never
      // affect query execution.
      LOG.debug("Failed to report PPL query to Query Insights", e);
    }
  }
}
