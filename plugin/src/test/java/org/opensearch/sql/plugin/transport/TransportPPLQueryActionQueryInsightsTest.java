/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.ThreadResourceAccounting;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.QueryInsightsMetadata;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Drives {@link TransportPPLQueryAction#doExecute} with only the engine stubbed, to pin what
 * reaches Query Insights: the record and each of its fields, the parent marker the child searches
 * see, and that the marker stays off the caller's thread and callback. Core's task resource
 * tracking is real, so the record's CPU, memory and timing come through the same path as on a node.
 */
public class TransportPPLQueryActionQueryInsightsTest {

  private static final String QUERY = "source=accounts | where age > 30 | fields firstname";
  private static final String ANONYMIZED = "source=accounts | where age > *** | fields firstname";
  private static final String USER_INFO = "admin||all_access|__user__";
  private static final long TASK_ID = 42L;
  private static final String MARKER = "PPL:node-1:42";
  private static final long LOOKAHEAD_HOLD_MILLIS = 500L;

  /** Registered by the Query Insights plugin, so absent when it is not installed. */
  private static final Setting<Boolean> PPL_RECORDING =
      Setting.boolSetting(
          TransportPPLQueryAction.QUERY_INSIGHTS_PPL_ENABLED_KEY,
          false,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
  private final PPLService pplService = mock(PPLService.class);
  private final ClusterService clusterService = mock(ClusterService.class);
  private final TransportService transportService = mock(TransportService.class);
  private final DiscoveryNode localNode = mock(DiscoveryNode.class);
  private final ExecutorService worker = Executors.newSingleThreadExecutor();
  private final ExecutorService backgroundPool = Executors.newSingleThreadExecutor();
  private final List<String> registeredMetrics = new ArrayList<>();

  private final List<BytesTransportRequest> reports = new CopyOnWriteArrayList<>();
  private final CountDownLatch reported = new CountDownLatch(1);
  private volatile long reportedAtNanos;

  private final CompletableFuture<Object> outcome = new CompletableFuture<>();
  private volatile long respondedAtNanos;
  private final AtomicReference<String> headerSeenByCaller = new AtomicReference<>("unset");
  private final AtomicReference<String> headerSeenByEngine = new AtomicReference<>("unset");

  private TaskResourceTrackingService tracking;
  private TransportPPLQueryAction action;

  @Before
  public void setUp() throws Exception {
    ThreadPool threadPool = mock(ThreadPool.class);
    when(threadPool.getThreadContext()).thenReturn(threadContext);
    NodeClient client = mock(NodeClient.class);
    when(client.threadPool()).thenReturn(threadPool);
    when(transportService.getThreadPool()).thenReturn(threadPool);
    when(localNode.getId()).thenReturn("node-1");
    when(clusterService.localNode()).thenReturn(localNode);
    doAnswer(
            invocation -> {
              reportedAtNanos = System.nanoTime();
              reports.add(invocation.getArgument(2));
              reported.countDown();
              return null;
            })
        .when(transportService)
        .sendRequest(eq(localNode), eq(QueryInsightsReporter.ACTION_NAME), any(), any());

    Settings trackingOn =
        Settings.builder()
            .put(TaskResourceTrackingService.TASK_RESOURCE_TRACKING_ENABLED.getKey(), true)
            .build();
    tracking =
        new TaskResourceTrackingService(
            trackingOn,
            new ClusterSettings(trackingOn, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool);

    Injector injector = mock(Injector.class);
    when(injector.getInstance(PPLService.class)).thenReturn(pplService);

    // Skips the constructor, which builds the plugin's Guice graph; doExecute reads only these.
    action = mock(TransportPPLQueryAction.class, CALLS_REAL_METHODS);
    setField("pplEnabled", (Supplier<Boolean>) () -> true);
    setField("injector", injector);
    setField("tracer", NoopTracer.INSTANCE);
    setField("clientRef", client);
    setField("clusterServiceRef", clusterService);
    setField("transportServiceRef", transportService);
    setField("taskResourceTrackingService", tracking);

    registerMetricIfAbsent(MetricName.PPL_REQ_TOTAL);
    registerMetricIfAbsent(MetricName.PPL_REQ_COUNT_TOTAL);

    // What the security plugin leaves on the request thread for an authenticated user.
    threadContext.putTransient("_opendistro_security_user_info", USER_INFO);
  }

  @After
  public void tearDown() {
    worker.shutdownNow();
    backgroundPool.shutdownNow();
    registeredMetrics.forEach(Metrics.getInstance()::unregisterMetric);
    OpenSearchQueryManager.clearCancellableTask();
  }

  @Test
  public void reportsTheCompletedQueryWithItsCostAndUser() throws Exception {
    recording(true);
    engine(
        (listener, queryTask) -> {
          burnCpu();
          listener.onResponse(emptyResponse());
        });

    PPLQueryTask task = execute();

    assertTrue("no record sent", reported.await(10, TimeUnit.SECONDS));
    assertTrue(outcome.get(10, TimeUnit.SECONDS) instanceof TransportPPLQueryResponse);
    Record record = onlyRecord();
    assertEquals("PPL", record.source());
    assertEquals(MARKER, record.parentMarker());
    assertEquals("node-1", record.nodeId());
    assertEquals("the query as submitted, literals included", QUERY, record.queryText());
    assertEquals(
        "the query's start, like a search record", task.getStartTime(), record.timestamp());
    assertTrue("engine CPU", record.cpuNanos() > 0);
    assertTrue("engine memory", record.memoryBytes() > 0);
    assertEquals("the totals _tasks shows", task.getCpuNanos(), record.cpuNanos());
    assertEquals(task.getAllocatedBytes(), record.memoryBytes());
    assertEquals(USER_INFO, record.userInfo());
    assertFalse(record.failed());
  }

  @Test
  public void childSearchesSeeTheMarkerButTheCallerDoesNot() throws Exception {
    recording(true);
    engine((listener, queryTask) -> listener.onResponse(emptyResponse()));

    execute();

    assertNull(
        "marker left on the calling thread",
        threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER));
    outcome.get(10, TimeUnit.SECONDS);
    assertEquals(
        "the engine's thread, which issues the child searches, carries the marker",
        MARKER,
        headerSeenByEngine.get());
    assertNull("marker leaked into the caller's callback", headerSeenByCaller.get());
  }

  @Test
  public void latencyIsTakenWhenTheResponseIsSent() throws Exception {
    recording(true);
    // A lookahead search the scanner started before the query answered, still running after.
    engine(
        (listener, queryTask) -> {
          CountDownLatch scoped = new CountDownLatch(1);
          CountDownLatch answered = new CountDownLatch(1);
          backgroundPool.execute(
              () -> {
                try (ThreadResourceAccounting.Scope ignored =
                    ThreadResourceAccounting.enter(queryTask)) {
                  scoped.countDown();
                  answered.await();
                  Thread.sleep(LOOKAHEAD_HOLD_MILLIS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
          scoped.await();
          listener.onResponse(emptyResponse());
          answered.countDown();
        });

    PPLQueryTask task = execute();

    assertTrue("no record sent", reported.await(10, TimeUnit.SECONDS));
    assertTrue(
        "the record waits for the lookahead's scope, so it goes out after the response",
        reportedAtNanos - respondedAtNanos
            >= TimeUnit.MILLISECONDS.toNanos(LOOKAHEAD_HOLD_MILLIS / 2));
    long respondedAtMillis = (respondedAtNanos - task.getStartTimeNanos()) / 1_000_000L;
    long latencyMillis = onlyRecord().latencyMillis();
    assertTrue(
        "latency "
            + latencyMillis
            + "ms includes time after the response, sent at "
            + respondedAtMillis
            + "ms",
        latencyMillis <= respondedAtMillis);
  }

  @Test
  public void failedQueryIsReportedAsFailed() throws Exception {
    recording(true);
    engine(
        (listener, queryTask) -> {
          burnCpu();
          listener.onFailure(new IllegalStateException("boom"));
        });

    execute();

    assertTrue("no record sent", reported.await(10, TimeUnit.SECONDS));
    assertTrue(outcome.get(10, TimeUnit.SECONDS) instanceof IllegalStateException);
    Record record = onlyRecord();
    assertTrue("failed flag", record.failed());
    assertEquals(QUERY, record.queryText());
    assertTrue("engine CPU", record.cpuNanos() > 0);
  }

  @Test
  public void recordingOffSendsNothingButStillTracksTheEngine() throws Exception {
    recording(false);
    engine(
        (listener, queryTask) -> {
          burnCpu();
          listener.onResponse(emptyResponse());
        });

    PPLQueryTask task = execute();
    outcome.get(10, TimeUnit.SECONDS);
    awaitEngine();

    assertTrue("record sent with recording off", reports.isEmpty());
    assertNull("marker stamped with recording off", headerSeenByEngine.get());
    assertTrue("_tasks and Live Queries still read the engine's CPU", task.getCpuNanos() > 0);
  }

  @Test
  public void queryInsightsNotInstalledSendsNothing() throws Exception {
    recording(null);
    engine((listener, queryTask) -> listener.onResponse(emptyResponse()));

    execute();
    outcome.get(10, TimeUnit.SECONDS);
    awaitEngine();

    assertTrue(reports.isEmpty());
    assertNull(headerSeenByEngine.get());
  }

  @Test
  public void nestedQueryKeepsTheOuterMarkerAndSendsNoRecord() throws Exception {
    recording(true);
    // An outer query's marker is already in the context, so the child searches roll up into it.
    threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, "PPL:node-0:7");
    engine((listener, queryTask) -> listener.onResponse(emptyResponse()));

    execute();
    outcome.get(10, TimeUnit.SECONDS);
    awaitEngine();

    assertEquals("PPL:node-0:7", headerSeenByEngine.get());
    assertEquals(
        "the outer marker survives on the calling thread",
        "PPL:node-0:7",
        threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER));
    assertTrue("a nested query must not report", reports.isEmpty());
  }

  /** What the engine does with a parsed query, on its worker thread, inside the query's scope. */
  private interface Engine {
    void run(ResponseListener<QueryResponse> listener, CancellableTask queryTask) throws Exception;
  }

  private void engine(Engine engine) {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              Consumer<QueryInsightsMetadata> sink = invocation.getArgument(3);
              // What PPLService does once the query parses.
              sink.accept(new QueryInsightsMetadata(ANONYMIZED, false));
              // What OpenSearchQueryManager.submit does: hand the query to a worker thread, which
              // carries the caller's thread context and runs inside a scope for the query's task.
              CancellableTask queryTask = OpenSearchQueryManager.getCancellableTask();
              worker.execute(
                  threadContext.preserveContext(
                      () -> {
                        headerSeenByEngine.set(
                            threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER));
                        try (ThreadResourceAccounting.Scope ignored =
                            ThreadResourceAccounting.enter(queryTask)) {
                          engine.run(listener, queryTask);
                        } catch (Exception e) {
                          listener.onFailure(e);
                        }
                      }));
              return null;
            })
        .when(pplService)
        .execute(any(), any(), any(), any());
  }

  /** Runs the query the way TransportAction.execute does: register, execute, unregister. */
  private PPLQueryTask execute() {
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest(
            new PPLQueryRequest(QUERY, new JSONObject().put("query", QUERY), "/_plugins/_ppl", ""));
    PPLQueryTask task =
        request.createTask(
            TASK_ID, "transport", PPLQueryAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
    // TaskManager.register: core tracks the task until its last thread is done. The context
    // startTracking stamps is closed at once, so the checks see only what doExecute leaves behind.
    tracking.startTracking(task).close();
    task.addResourceTrackingCompletionListener(
        new NotifyOnceListener<>() {
          @Override
          protected void innerOnResponse(Task completed) {
            tracking.stopTracking(completed);
          }

          @Override
          protected void innerOnFailure(Exception e) {}
        });
    action.doExecute(task, request, caller(task));
    return task;
  }

  /** The listener TransportAction.execute puts in front of the caller's: unregister, answer. */
  private ActionListener<TransportPPLQueryResponse> caller(PPLQueryTask task) {
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse response) {
        finish(task, response);
      }

      @Override
      public void onFailure(Exception e) {
        finish(task, e);
      }
    };
  }

  private void finish(PPLQueryTask task, Object result) {
    // TaskManager.unregister releases the task's own thread.
    task.decrementResourceTrackingThreads();
    respondedAtNanos = System.nanoTime();
    headerSeenByCaller.set(threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER));
    outcome.complete(result);
  }

  private void recording(Boolean enabled) {
    ClusterSettings settings =
        enabled == null
            ? new ClusterSettings(Settings.EMPTY, Set.of())
            : new ClusterSettings(
                Settings.builder().put(PPL_RECORDING.getKey(), enabled).build(),
                Set.<Setting<?>>of(PPL_RECORDING));
    when(clusterService.getClusterSettings()).thenReturn(settings);
  }

  /** Waits for the engine's scope to close, after which no record can still be on its way. */
  private void awaitEngine() throws InterruptedException {
    worker.shutdown();
    assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
  }

  /** A record's fields, in the order {@link QueryInsightsReporter} writes them. */
  private record Record(
      String source,
      String parentMarker,
      String nodeId,
      String queryText,
      long timestamp,
      long latencyMillis,
      long cpuNanos,
      long memoryBytes,
      String userInfo,
      boolean failed) {}

  private Record onlyRecord() throws IOException {
    assertEquals("records sent", 1, reports.size());
    try (StreamInput in = reports.get(0).bytes().streamInput()) {
      assertEquals(QueryInsightsReporter.FORMAT_VERSION, in.readVInt());
      Record record =
          new Record(
              in.readString(),
              in.readString(),
              in.readString(),
              in.readString(),
              in.readVLong(),
              in.readVLong(),
              in.readVLong(),
              in.readVLong(),
              in.readString(),
              in.readBoolean());
      assertEquals("trailing bytes", 0, in.available());
      return record;
    }
  }

  private static QueryResponse emptyResponse() {
    return new QueryResponse(new Schema(List.of()), List.of(), Cursor.None);
  }

  private static void burnCpu() {
    long x = 0;
    for (int i = 0; i < 1_000_000; i++) {
      x += Integer.toString(i).hashCode();
    }
    assertNotEquals(42, x);
  }

  private void registerMetricIfAbsent(MetricName name) {
    if (Metrics.getInstance().getMetric(name.getName()) == null) {
      Metrics.getInstance().registerMetric(new NumericMetric<>(name.getName(), new BasicCounter()));
      registeredMetrics.add(name.getName());
    }
  }

  private void setField(String name, Object value) throws Exception {
    Field field = TransportPPLQueryAction.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(action, value);
  }
}
