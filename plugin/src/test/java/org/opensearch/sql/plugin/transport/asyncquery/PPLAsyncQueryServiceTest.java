/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mockito.InOrder;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.PPLQueryTask;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskManager;

public class PPLAsyncQueryServiceTest {
  private static final PPLAsyncQueryUser OWNER = PPLAsyncQueryUser.UNSECURED;
  private static final TimeValue KEEP_ALIVE = TimeValue.timeValueMinutes(5);

  private final AtomicLong now = new AtomicLong(1_000);
  private final AtomicReference<Runnable> timeoutTask = new AtomicReference<>();
  private final AtomicBoolean timeoutCancelled = new AtomicBoolean();
  private final PPLAsyncQueryService service = service(20, 100);

  @Test
  public void fastSuccessReturnsDirectResultWithoutRetainingJob() {
    AtomicReference<PPLAsyncQueryService.JobSnapshot> result = new AtomicReference<>();
    AtomicInteger responses = new AtomicInteger();
    TrackingExecution execution = new TrackingExecution(response(2));
    startQuery(
        service,
        null,
        TimeValue.timeValueSeconds(5),
        execution,
        listener(
            snapshot -> {
              result.set(snapshot);
              responses.incrementAndGet();
            }));
    now.addAndGet(25);
    execution.complete();

    assertEquals(1, responses.get());
    assertEquals(
        new PPLAsyncQueryService.JobSnapshot.Succeeded(Optional.empty(), response(2), 25),
        result.get());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
    assertEquals(1, execution.reads.get());
    assertEquals(1, execution.closes.get());
    assertTrue(timeoutCancelled.get());

    timeoutTask.get().run();
    assertEquals(1, responses.get());
  }

  @Test
  public void timeoutReturnsIdAndLaterGetReturnsCompleteResult() {
    AtomicReference<PPLAsyncQueryService.JobSnapshot> retainedResponse = new AtomicReference<>();
    TrackingExecution execution = new TrackingExecution(null);
    startQuery(
        service, null, TimeValue.timeValueSeconds(5), execution, listener(retainedResponse::set));

    timeoutTask.get().run();

    String id = retainedResponse.get().id().orElseThrow();
    assertEquals(
        new PPLAsyncQueryService.JobSnapshot.Running(id, Optional.empty()), retainedResponse.get());
    assertEquals(1, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());

    now.addAndGet(25);
    execution.succeed(response(2));
    PPLAsyncQueryService.JobSnapshot completed = service.get(id, OWNER, null);

    assertEquals(
        new PPLAsyncQueryService.JobSnapshot.Succeeded(Optional.of(id), response(2), 25),
        completed);
    assertEquals(0, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());
    assertEquals(0, execution.closes.get());
  }

  @Test
  public void retentionWaitPreventsExpiryAndLeaseStartsWhenIdIsReturned() {
    AtomicReference<PPLAsyncQueryService.JobSnapshot> retainedResponse = new AtomicReference<>();
    startQuery(
        service,
        null,
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(5),
        new TrackingExecution(null),
        listener(retainedResponse::set));

    now.addAndGet(TimeValue.timeValueSeconds(2).millis());
    service.reapExpired();

    assertNull(retainedResponse.get());
    assertEquals(1, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());

    timeoutTask.get().run();
    assertEquals(PPLAsyncQueryService.Status.RUNNING, retainedResponse.get().status());

    now.addAndGet(TimeValue.timeValueSeconds(1).millis() + 1);
    service.reapExpired();
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void fastFailureReturnsDirectFailureWithoutId() {
    AtomicReference<Exception> failure = new AtomicReference<>();
    TrackingExecution execution = new TrackingExecution(response(1));
    startQuery(
        service,
        null,
        TimeValue.timeValueSeconds(5),
        execution,
        ActionListener.wrap(
            ignored -> {
              throw new AssertionError("Expected direct query failure");
            },
            failure::set));

    execution.fail(new IllegalStateException("boom"));

    assertEquals("boom", failure.get().getMessage());
    assertEquals(0, execution.reads.get());
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void expiredGetCancelsAndRemovesJob() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    TrackingExecution execution = new TrackingExecution(null);
    String id = startRetainedQuery(service, task, execution);

    now.addAndGet(KEEP_ALIVE.millis());

    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
    verify(task).cancel("PPL asynchronous query expired");
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void missingLocalJobReturnsNotFound() {
    String remoteId = PPLAsyncQueryJobId.create("node-b").encode();

    assertThrows(ResourceNotFoundException.class, () -> service.get(remoteId, OWNER, null));
    assertThrows(ResourceNotFoundException.class, () -> service.delete(remoteId, OWNER));
  }

  @Test
  public void deleteCancelsRunningJobAndReleasesState() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    TrackingExecution execution = new TrackingExecution(null);
    String id = startRetainedQuery(service, task, execution);

    PPLAsyncQueryService.DeleteResult result = service.delete(id, OWNER);

    assertEquals(
        new PPLAsyncQueryService.DeleteResult(id, PPLAsyncQueryService.Status.CANCELLED), result);
    verify(task).cancel("PPL asynchronous query cancelled by user");
    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void deleteReturnsExistingTerminalStatus() {
    CancellableTask task = mock(CancellableTask.class);
    TrackingExecution execution = new TrackingExecution(response(1));
    String id = startRetainedQuery(service, task, execution);
    execution.complete();

    PPLAsyncQueryService.DeleteResult result = service.delete(id, OWNER);

    assertEquals(
        new PPLAsyncQueryService.DeleteResult(id, PPLAsyncQueryService.Status.SUCCEEDED), result);
    verify(task, never()).cancel(org.mockito.ArgumentMatchers.anyString());
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void cancellationUsesTaskManagerWhenAttached() {
    TaskManager taskManager = mock(TaskManager.class);
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    service.attachTaskManager(taskManager);
    String id = startRetainedQuery(service, task, new TrackingExecution(null));

    service.delete(id, OWNER);

    verify(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.eq("PPL asynchronous query cancelled by user"),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void startRegistersTaskAndCompletionReleasesIt() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    RequestTaskRegistration registration = registerRequestTask(taskManager, request, task);
    service.attachTaskManager(taskManager);

    TrackingExecution execution = new TrackingExecution(null);
    service.start(
        OWNER,
        KEEP_ALIVE,
        TimeValue.ZERO,
        request,
        registration.requestTask(),
        ignored -> execution,
        listener(snapshot -> assertEquals(PPLAsyncQueryService.Status.RUNNING, snapshot.status())));
    execution.succeed(response(1));

    InOrder registrationOrder = inOrder(taskManager);
    registrationOrder
        .verify(taskManager)
        .registerChildNode(registration.requestTask().getId(), registration.localNode());
    registrationOrder.verify(taskManager).register("transport", PPLQueryAction.NAME, request);
    assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
    verify(taskManager).unregister(task);
    verify(registration.childNodeRegistration()).close();
    assertEquals(0, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());
  }

  @Test
  public void executionStartFailureCompletesJobAndReleasesTask() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    RequestTaskRegistration registration = registerRequestTask(taskManager, request, task);
    service.attachTaskManager(taskManager);
    AtomicReference<Exception> failure = new AtomicReference<>();

    service.start(
        OWNER,
        KEEP_ALIVE,
        TimeValue.timeValueSeconds(5),
        request,
        registration.requestTask(),
        ignored -> {
          throw new IllegalStateException("execution did not start");
        },
        ActionListener.wrap(
            ignored -> {
              throw new AssertionError("Expected execution startup failure");
            },
            failure::set));

    assertEquals("execution did not start", failure.get().getMessage());
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void deleteCancelsAndReleasesServiceOwnedTask() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    when(task.isCancelled()).thenReturn(false);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    RequestTaskRegistration registration = registerRequestTask(taskManager, request, task);
    doAnswer(
            invocation -> {
              ActionListener<Void> listener = invocation.getArgument(3);
              listener.onResponse(null);
              return null;
            })
        .when(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
    service.attachTaskManager(taskManager);
    AtomicReference<String> id = new AtomicReference<>();

    service.start(
        OWNER,
        KEEP_ALIVE,
        TimeValue.ZERO,
        request,
        registration.requestTask(),
        ignored -> new TrackingExecution(null),
        listener(snapshot -> id.set(snapshot.id().orElseThrow())));

    service.delete(id.get(), OWNER);

    verify(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.eq("PPL asynchronous query cancelled by user"),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
  }

  @Test
  public void startupFailureAfterJobCreationReleasesServiceOwnedTask() {
    PPLAsyncQueryService abortingService =
        new PPLAsyncQueryService(
            "node-a",
            now::get,
            (delay, task) -> {
              throw new IllegalStateException("scheduler unavailable");
            },
            () -> 20,
            () -> 100,
            () -> TimeValue.timeValueSeconds(60),
            () -> TimeValue.timeValueHours(24));
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    when(task.isCancelled()).thenReturn(true);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    RequestTaskRegistration registration = registerRequestTask(taskManager, request, task);
    abortingService.attachTaskManager(taskManager);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                abortingService.start(
                    OWNER,
                    KEEP_ALIVE,
                    TimeValue.timeValueSeconds(5),
                    request,
                    registration.requestTask(),
                    ignored -> new TrackingExecution(null),
                    listener(snapshot -> {})));

    assertEquals("scheduler unavailable", failure.getMessage());
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
    assertEquals(0, abortingService.runningQueryCount());
    assertEquals(0, abortingService.retainedJobCount());
  }

  @Test
  public void childTrackingFailurePreventsRetainedTaskRegistration() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask requestTask = mock(PPLQueryTask.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    when(requestTask.getId()).thenReturn(42L);
    when(taskManager.localNode()).thenReturn(localNode);
    when(taskManager.registerChildNode(42L, localNode))
        .thenThrow(new IllegalStateException("channel closed"));
    service.attachTaskManager(taskManager);

    assertThrows(
        IllegalStateException.class,
        () ->
            service.start(
                OWNER,
                KEEP_ALIVE,
                TimeValue.timeValueSeconds(5),
                request,
                requestTask,
                ignored -> new TrackingExecution(null),
                listener(snapshot -> {})));

    verify(taskManager, never()).register("transport", PPLQueryAction.NAME, request);
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void rejectsUnauthorizedCallerWithoutRenewingOrDeleting() {
    PPLAsyncQueryUser securedOwner = new PPLAsyncQueryUser("alice", "tenant", List.of("role-a"));
    PPLAsyncQueryUser otherUser = new PPLAsyncQueryUser("bob", "tenant", List.of("role-a"));
    AtomicReference<String> id = new AtomicReference<>();
    service.start(
        securedOwner,
        KEEP_ALIVE,
        TimeValue.ZERO,
        jobTask(null),
        ignored -> new TrackingExecution(null),
        listener(snapshot -> id.set(snapshot.id().orElseThrow())));

    assertThrows(OpenSearchSecurityException.class, () -> service.get(id.get(), otherUser, null));
    assertThrows(OpenSearchSecurityException.class, () -> service.delete(id.get(), otherUser));
    assertEquals(
        PPLAsyncQueryService.Status.RUNNING, service.get(id.get(), securedOwner, null).status());
  }

  @Test
  public void enforcesRunningAndRetainedCapacity() {
    PPLAsyncQueryService limited = service(1, 1);
    limited.start(
        OWNER,
        KEEP_ALIVE,
        TimeValue.ZERO,
        jobTask(null),
        ignored -> new TrackingExecution(null),
        listener(snapshot -> {}));

    OpenSearchStatusException exception =
        assertThrows(
            OpenSearchStatusException.class,
            () ->
                limited.start(
                    OWNER,
                    KEEP_ALIVE,
                    TimeValue.ZERO,
                    jobTask(null),
                    ignored -> new TrackingExecution(null),
                    listener(snapshot -> {})));

    assertEquals(429, exception.status().getStatus());
  }

  @Test
  public void createFailureReleasesReservedCapacity() {
    PPLAsyncQueryService missingOwnerNode =
        new PPLAsyncQueryService(
            (String) null,
            now::get,
            (delay, task) -> () -> {},
            () -> 1,
            () -> 1,
            () -> TimeValue.timeValueSeconds(60),
            () -> TimeValue.timeValueHours(24));

    assertThrows(
        NullPointerException.class,
        () ->
            missingOwnerNode.start(
                OWNER,
                KEEP_ALIVE,
                TimeValue.ZERO,
                jobTask(null),
                ignored -> new TrackingExecution(null),
                listener(snapshot -> {})));

    assertEquals(0, missingOwnerNode.runningQueryCount());
    assertEquals(0, missingOwnerNode.retainedJobCount());
  }

  @Test
  public void finalSnapshotDefensivelyCopiesRows() {
    AtomicReference<PPLAsyncQueryService.JobSnapshot> result = new AtomicReference<>();
    List<org.opensearch.sql.data.model.ExprValue> rows = new ArrayList<>();
    rows.add(ExprValueUtils.stringValue("first"));
    QueryResponse response =
        new QueryResponse(
            new Schema(List.of(new Column("state", null, ExprCoreType.STRING))), rows, null);

    TrackingExecution execution = new TrackingExecution(response);
    startQuery(service, null, TimeValue.timeValueSeconds(5), execution, listener(result::set));
    execution.complete();
    rows.add(ExprValueUtils.stringValue("second"));

    assertTrue(result.get() instanceof PPLAsyncQueryService.JobSnapshot.Succeeded);
    PPLAsyncQueryService.JobSnapshot.Succeeded succeeded =
        (PPLAsyncQueryService.JobSnapshot.Succeeded) result.get();
    assertEquals(1, succeeded.response().getResults().size());
  }

  @Test
  public void completedExecutionCanBeAttachedBeforeCompletionIsObserved() {
    AtomicReference<PPLAsyncQueryService.JobSnapshot> result = new AtomicReference<>();
    TrackingExecution execution = new TrackingExecution(null);

    execution.succeed(response(2));
    startQuery(service, null, TimeValue.timeValueSeconds(5), execution, listener(result::set));

    assertEquals(
        new PPLAsyncQueryService.JobSnapshot.Succeeded(Optional.empty(), response(2), 0),
        result.get());
    assertEquals(1, execution.closes.get());
  }

  @Test
  public void runningGetMaterializesCurrentResultOutsideJob() {
    TrackingExecution execution = new TrackingExecution(response(1));
    String id = startRetainedQuery(service, null, execution);

    PPLAsyncQueryService.JobSnapshot first = service.get(id, OWNER, null);
    execution.setCurrent(response(3));
    PPLAsyncQueryService.JobSnapshot second = service.get(id, OWNER, null);

    assertEquals(new PPLAsyncQueryService.JobSnapshot.Running(id, Optional.of(response(1))), first);
    assertEquals(
        new PPLAsyncQueryService.JobSnapshot.Running(id, Optional.of(response(3))), second);
  }

  @Test
  public void failedRetainedJobReturnsNoProvisionalRowsAndClosesExecution() {
    NumericMetric<Long> failures =
        new NumericMetric<>(MetricName.PPL_FAILED_REQ_COUNT_SYS.getName(), new BasicCounter());
    Metrics.getInstance().registerMetric(failures);
    try {
      TrackingExecution execution = new TrackingExecution(response(1));
      String id = startRetainedQuery(service, null, execution);

      execution.fail(new IllegalStateException("boom"));
      PPLAsyncQueryService.JobSnapshot failed = service.get(id, OWNER, null);

      assertEquals(
          new PPLAsyncQueryService.JobSnapshot.Failed(
              Optional.of(id),
              new PPLAsyncQueryService.Failure("IllegalStateException", "boom"),
              0),
          failed);
      assertEquals(0, execution.reads.get());
      assertEquals(1, execution.closes.get());
    } finally {
      Metrics.getInstance().unregisterMetric(failures.getName());
    }
  }

  @Test
  public void failedRetainedJobRecordsFailureMetric() {
    NumericMetric<Long> failures =
        new NumericMetric<>(MetricName.PPL_FAILED_REQ_COUNT_CUS.getName(), new BasicCounter());
    Metrics.getInstance().registerMetric(failures);
    try {
      TrackingExecution execution = new TrackingExecution(null);
      startRetainedQuery(service, null, execution);

      execution.fail(new IllegalArgumentException("invalid query"));

      assertEquals(Long.valueOf(1), failures.getValue());
    } finally {
      Metrics.getInstance().unregisterMetric(failures.getName());
    }
  }

  @Test
  public void deleteBeforeExecutionAttachmentClosesLateHandle() {
    TrackingExecution execution = new TrackingExecution(response(1));
    AtomicReference<String> id = new AtomicReference<>();

    service.start(
        OWNER,
        KEEP_ALIVE,
        TimeValue.ZERO,
        jobTask(null),
        ignored -> {
          service.delete(id.get(), OWNER);
          return execution;
        },
        listener(snapshot -> id.set(snapshot.id().orElseThrow())));

    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id.get(), OWNER, null));
  }

  @Test
  public void concurrentGetDoesNotBlockDeleteOnResultMaterialization() throws Exception {
    BlockingExecution execution = new BlockingExecution(response(1));
    String id = startRetainedQuery(service, null, execution);

    CompletableFuture<PPLAsyncQueryService.JobSnapshot> get =
        CompletableFuture.supplyAsync(() -> service.get(id, OWNER, null));
    assertTrue(execution.readStarted.await(5, TimeUnit.SECONDS));
    CompletableFuture<PPLAsyncQueryService.DeleteResult> delete =
        CompletableFuture.supplyAsync(() -> service.delete(id, OWNER));

    try {
      assertEquals(PPLAsyncQueryService.Status.CANCELLED, delete.get(5, TimeUnit.SECONDS).status());
    } finally {
      execution.allowRead.countDown();
    }

    assertEquals(PPLAsyncQueryService.Status.RUNNING, get.get(5, TimeUnit.SECONDS).status());
    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
  }

  @Test
  public void shutdownClosesRetainedExecutionExactlyOnce() throws Exception {
    TrackingExecution execution = new TrackingExecution(response(1));
    startRetainedQuery(service, null, execution);

    service.close();
    service.close();

    assertEquals(1, execution.closes.get());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void successfulCompletionRequiresFinalResultToBeVisible() {
    AtomicReference<Exception> failure = new AtomicReference<>();
    TrackingExecution execution = new TrackingExecution(null);
    startQuery(
        service,
        null,
        TimeValue.timeValueSeconds(5),
        execution,
        ActionListener.wrap(snapshot -> {}, failure::set));

    execution.complete();

    assertTrue(failure.get() instanceof IllegalStateException);
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void retentionResponseMaterializationFailureAbortsUndeliverableJob() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    AtomicReference<Exception> failure = new AtomicReference<>();
    ThrowingExecution execution = new ThrowingExecution();
    startQuery(
        service,
        task,
        TimeValue.timeValueSeconds(5),
        execution,
        ActionListener.wrap(snapshot -> {}, failure::set));

    timeoutTask.get().run();

    assertTrue(failure.get() instanceof IllegalStateException);
    verify(task).cancel("PPL asynchronous query startup failed");
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void validatesDurationBounds() {
    assertThrows(IllegalArgumentException.class, () -> service.validateKeepAlive(TimeValue.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> service.validateKeepAlive(TimeValue.timeValueHours(25)));
    assertThrows(
        IllegalArgumentException.class,
        () -> service.validateWaitForCompletion(TimeValue.timeValueSeconds(61)));

    service.validateWaitForCompletion(TimeValue.ZERO);
    service.validateWaitForCompletion(TimeValue.timeValueSeconds(60));
    service.validateKeepAlive(TimeValue.timeValueHours(24));
  }

  private PPLAsyncQueryService service(int maxRunning, int maxRetained) {
    return new PPLAsyncQueryService(
        "node-a",
        now::get,
        (delay, task) -> {
          timeoutTask.set(task);
          timeoutCancelled.set(false);
          return () -> timeoutCancelled.set(true);
        },
        () -> maxRunning,
        () -> maxRetained,
        () -> TimeValue.timeValueSeconds(60),
        () -> TimeValue.timeValueHours(24));
  }

  private String startRetainedQuery(
      PPLAsyncQueryService targetService, CancellableTask task, AsyncQueryExecution execution) {
    AtomicReference<String> id = new AtomicReference<>();
    startQuery(
        targetService,
        task,
        TimeValue.ZERO,
        execution,
        listener(snapshot -> id.set(snapshot.id().orElseThrow())));
    return id.get();
  }

  private void startQuery(
      PPLAsyncQueryService targetService,
      CancellableTask task,
      TimeValue waitForCompletion,
      AsyncQueryExecution execution,
      ActionListener<PPLAsyncQueryService.JobSnapshot> responseListener) {
    startQuery(targetService, task, KEEP_ALIVE, waitForCompletion, execution, responseListener);
  }

  private void startQuery(
      PPLAsyncQueryService targetService,
      CancellableTask task,
      TimeValue keepAlive,
      TimeValue waitForCompletion,
      AsyncQueryExecution execution,
      ActionListener<PPLAsyncQueryService.JobSnapshot> responseListener) {
    targetService.start(
        OWNER, keepAlive, waitForCompletion, jobTask(task), ignored -> execution, responseListener);
  }

  private static PPLAsyncQueryJob.JobTask jobTask(CancellableTask task) {
    return new PPLAsyncQueryJob.JobTask(task, () -> {});
  }

  private static RequestTaskRegistration registerRequestTask(
      TaskManager taskManager, TransportPPLQueryRequest request, PPLQueryTask retainedTask) {
    PPLQueryTask requestTask = mock(PPLQueryTask.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    Releasable childNodeRegistration = mock(Releasable.class);
    when(requestTask.getId()).thenReturn(42L);
    when(localNode.getId()).thenReturn("node-a");
    when(taskManager.localNode()).thenReturn(localNode);
    when(taskManager.registerChildNode(42L, localNode)).thenReturn(childNodeRegistration);
    when(taskManager.register("transport", PPLQueryAction.NAME, request))
        .thenAnswer(
            invocation -> {
              assertEquals(new TaskId("node-a", 42L), request.getParentTask());
              return retainedTask;
            });
    return new RequestTaskRegistration(requestTask, localNode, childNodeRegistration);
  }

  private record RequestTaskRegistration(
      PPLQueryTask requestTask, DiscoveryNode localNode, Releasable childNodeRegistration) {}

  private static ActionListener<PPLAsyncQueryService.JobSnapshot> listener(
      java.util.function.Consumer<PPLAsyncQueryService.JobSnapshot> consumer) {
    return ActionListener.wrap(
        snapshot -> consumer.accept(snapshot),
        failure -> {
          throw new AssertionError(failure);
        });
  }

  private static QueryResponse response(int rowCount) {
    Schema schema = new Schema(List.of(new Column("state", null, ExprCoreType.STRING)));
    return new QueryResponse(
        schema,
        java.util.stream.IntStream.range(0, rowCount)
            .mapToObj(i -> ExprValueUtils.stringValue("state-" + i))
            .toList(),
        null);
  }

  private static final class TrackingExecution implements AsyncQueryExecution {
    private final AtomicReference<QueryResponse> current;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();

    private TrackingExecution(QueryResponse current) {
      this.current = new AtomicReference<>(current);
    }

    private void setCurrent(QueryResponse response) {
      current.set(response);
    }

    private void succeed(QueryResponse response) {
      current.set(response);
      completion.complete(null);
    }

    private void complete() {
      completion.complete(null);
    }

    private void fail(Exception failure) {
      completion.completeExceptionally(failure);
    }

    @Override
    public Optional<QueryResponse> currentResult() {
      reads.incrementAndGet();
      return Optional.ofNullable(current.get());
    }

    @Override
    public CompletionStage<Void> completion() {
      return completion;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closes.incrementAndGet();
      }
    }
  }

  private static final class BlockingExecution implements AsyncQueryExecution {
    private final QueryResponse response;
    private final CountDownLatch readStarted = new CountDownLatch(1);
    private final CountDownLatch allowRead = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger closes = new AtomicInteger();

    private BlockingExecution(QueryResponse response) {
      this.response = response;
    }

    @Override
    public Optional<QueryResponse> currentResult() {
      readStarted.countDown();
      try {
        assertTrue(allowRead.await(5, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
      return Optional.of(response);
    }

    @Override
    public CompletionStage<Void> completion() {
      return new CompletableFuture<>();
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closes.incrementAndGet();
      }
    }
  }

  private static final class ThrowingExecution implements AsyncQueryExecution {
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger closes = new AtomicInteger();

    @Override
    public Optional<QueryResponse> currentResult() {
      throw new IllegalStateException("materialization failed");
    }

    @Override
    public CompletionStage<Void> completion() {
      return new CompletableFuture<>();
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closes.incrementAndGet();
      }
    }
  }
}
