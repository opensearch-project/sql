/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Pins the wire format {@link QueryInsightsReporter} sends to Query Insights. The reader lives in a
 * different plugin ({@code ReportQueryBytesRequestHandler}) and the two share no classes, so the
 * byte layout is the contract. The golden-bytes test fails on any one-sided change to the layout;
 * the consumer-order test documents what each field is and would catch a reorder.
 */
public class QueryInsightsReporterTest {

  private static final String ACTION = "cluster:admin/opensearch/query_insights/report_query_bytes";

  /**
   * Payload for the inputs in {@link #reportSample}; recompute only on a deliberate format bump.
   */
  private static final String GOLDEN_BASE64 =
      "AQNQUEwQUFBMOm5vZGUtMToxMjM0NQZub2RlLTElc291cmNlPXRhYmxlIHwgd2hlcmUgaWRlbnRpZmllciA+ICoqKo"
          + "DQlf+8MYgIwMjmxgLwzaunAQIGZW50aXR5CWVtcGxveWVlcxphZG1pbnx8YWxsX2FjY2Vzc3xfX3VzZXJfXwE=";

  private TransportService transportService;
  private DiscoveryNode localNode;
  private ThreadContext threadContext;
  private final AtomicReference<BytesTransportRequest> captured = new AtomicReference<>();
  private final AtomicReference<String> actionSeen = new AtomicReference<>();
  private final AtomicReference<String> headerSeenDuringSend = new AtomicReference<>("unset");

  @Before
  public void setUp() {
    transportService = mock(TransportService.class);
    localNode = mock(DiscoveryNode.class);
    threadContext = new ThreadContext(Settings.EMPTY);
    ThreadPool threadPool = mock(ThreadPool.class);
    when(threadPool.getThreadContext()).thenReturn(threadContext);
    when(transportService.getThreadPool()).thenReturn(threadPool);
    doAnswer(
            invocation -> {
              actionSeen.set(invocation.getArgument(1));
              captured.set(invocation.getArgument(2));
              headerSeenDuringSend.set(threadContext.getHeader("caller-header"));
              return null;
            })
        .when(transportService)
        .sendRequest(eq(localNode), any(String.class), any(), any());
  }

  /** Fixed inputs covering every field, including a multi-index list and a user string. */
  private void reportSample() {
    QueryInsightsReporter.report(
        transportService,
        localNode,
        "PPL",
        "PPL:node-1:12345",
        "node-1",
        "source=table | where identifier > ***",
        1_700_000_000_000L,
        1032L,
        685_352_000L,
        350_938_864L,
        Arrays.asList("entity", "employees"),
        "admin||all_access|__user__",
        true);
  }

  @Test
  public void goldenBytes() {
    reportSample();
    assertEquals(ACTION, actionSeen.get());
    byte[] bytes = BytesReference.toBytes(captured.get().bytes());
    assertEquals(
        "wire layout changed; if deliberate, bump FORMAT_VERSION on both plugins and update the"
            + " golden value",
        GOLDEN_BASE64,
        Base64.getEncoder().encodeToString(bytes));
  }

  @Test
  public void decodesInConsumerOrder() throws IOException {
    reportSample();
    // Mirrors ReportQueryBytesRequestHandler#deserialize field for field.
    try (StreamInput in = captured.get().bytes().streamInput()) {
      assertEquals(QueryInsightsReporter.FORMAT_VERSION, in.readVInt());
      assertEquals("PPL", in.readString()); // querySource
      assertEquals("PPL:node-1:12345", in.readString()); // coordinatorId / parent marker
      assertEquals("node-1", in.readString()); // nodeId
      assertEquals("source=table | where identifier > ***", in.readString()); // queryText
      assertEquals(1_700_000_000_000L, in.readVLong()); // timestampMillis
      assertEquals(1032L, in.readVLong()); // latencyMillis
      assertEquals(685_352_000L, in.readVLong()); // cpuNanos
      assertEquals(350_938_864L, in.readVLong()); // memoryBytes
      int indexCount = in.readVInt();
      String[] indices = new String[indexCount];
      for (int i = 0; i < indexCount; i++) {
        indices[i] = in.readString();
      }
      assertArrayEquals(new String[] {"entity", "employees"}, indices);
      assertEquals("admin||all_access|__user__", in.readString()); // userInfo
      assertTrue(in.readBoolean()); // failed
      assertEquals("trailing bytes would be misread by the consumer", 0, in.available());
    }
  }

  @Test
  public void formatVersionIsOne() {
    // The consumer rejects any other value; bumping this is a cross-plugin change.
    assertEquals(1, QueryInsightsReporter.FORMAT_VERSION);
  }

  @Test
  public void nullsBecomeEmptyStringsAndNegativesClampToZero() throws IOException {
    QueryInsightsReporter.report(
        transportService, localNode, null, null, null, null, 5L, -1L, -2L, -3L, null, null, false);
    try (StreamInput in = captured.get().bytes().streamInput()) {
      in.readVInt();
      assertEquals("", in.readString());
      assertEquals("", in.readString());
      assertEquals("", in.readString());
      assertEquals("", in.readString());
      assertEquals(5L, in.readVLong());
      assertEquals(0L, in.readVLong());
      assertEquals(0L, in.readVLong());
      assertEquals(0L, in.readVLong());
      assertEquals(0, in.readVInt()); // null index list -> empty
      assertEquals("", in.readString());
      assertFalse(in.readBoolean());
    }
  }

  @Test
  public void nullIndexEntryBecomesEmptyString() throws IOException {
    QueryInsightsReporter.report(
        transportService,
        localNode,
        "PPL",
        "m",
        "n",
        "q",
        1L,
        1L,
        1L,
        1L,
        Arrays.asList("a", null, "b"),
        "",
        false);
    try (StreamInput in = captured.get().bytes().streamInput()) {
      in.readVInt();
      for (int i = 0; i < 4; i++) {
        in.readString();
      }
      for (int i = 0; i < 4; i++) {
        in.readVLong();
      }
      assertEquals(3, in.readVInt());
      assertEquals(
          List.of("a", "", "b"), List.of(in.readString(), in.readString(), in.readString()));
    }
  }

  @Test
  public void sendRunsWithCallerContextStashed() {
    // The send is an internal cluster:admin action; it must not be authorized against the end
    // user, so the caller's context is stashed for the duration of the send and restored after.
    threadContext.putHeader("caller-header", "present");
    reportSample();
    assertNull("send must not see the caller's context", headerSeenDuringSend.get());
    assertEquals(
        "caller's context must be restored", "present", threadContext.getHeader("caller-header"));
  }

  @Test
  public void sendFailureDoesNotPropagate() {
    doThrow(new RuntimeException("transport down"))
        .when(transportService)
        .sendRequest(eq(localNode), any(String.class), any(), any());
    // Reporting is best-effort; a failure here must never surface to the query that just finished.
    reportSample();
  }
}
