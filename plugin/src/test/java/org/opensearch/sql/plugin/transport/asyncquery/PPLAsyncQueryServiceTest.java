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
import java.util.function.Function;
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
    AsyncQueryScenario scenario =
        scenario()
            .withCurrentResult(response(2))
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start();

    scenario.completeAfterMillis(25);

    scenario
        .assertDirectSuccess(response(2), 25)
        .assertNotRetained()
        .assertExecutionReadOnceAndClosed()
        .assertTimeoutCancelled();

    scenario.fireTimeout().assertResponseCount(1);
  }

  @Test
  public void timeoutReturnsIdAndLaterGetReturnsCompleteResult() {
    AsyncQueryScenario scenario =
        scenario()
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start()
            .fireTimeout()
            .assertRetainedRunning(Optional.empty())
            .assertCapacity(1, 1);

    scenario
        .succeedAfterMillis(25, response(2))
        .get()
        .assertGetResponse(
            new PPLAsyncQueryService.JobSnapshot.Succeeded(
                Optional.of(scenario.id()), response(2), 25))
        .assertCapacity(0, 1)
        .assertExecutionReadCount(2)
        .assertExecutionCloseCount(0);
  }

  @Test
  public void retentionWaitPreventsExpiryAndLeaseStartsWhenIdIsReturned() {
    AsyncQueryScenario scenario =
        scenario()
            .withKeepAlive(TimeValue.timeValueSeconds(1))
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start();

    scenario
        .advanceMillis(TimeValue.timeValueSeconds(2).millis())
        .reapExpired()
        .assertNoInitialResponse()
        .assertCapacity(1, 1);

    scenario.fireTimeout().assertRetainedRunning(Optional.empty());

    scenario
        .advanceMillis(TimeValue.timeValueSeconds(1).millis() + 1)
        .reapExpired()
        .assertCapacity(0, 0);
  }

  @Test
  public void fastFailureReturnsDirectFailureWithoutId() {
    scenario()
        .withCurrentResult(response(1))
        .withWaitForCompletion(TimeValue.timeValueSeconds(5))
        .start()
        .fail(new IllegalStateException("boom"))
        .assertFailure(IllegalStateException.class, "boom")
        .assertExecutionReadCount(0)
        .assertExecutionCloseCount(1)
        .assertCapacity(0, 0);
  }

  @Test
  public void expiredGetCancelsAndRemovesJob() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    AsyncQueryScenario scenario = scenario().withTask(task).start();

    scenario.advanceMillis(KEEP_ALIVE.millis());

    assertThrows(ResourceNotFoundException.class, scenario::get);
    verify(task).cancel("PPL asynchronous query expired");
    scenario.assertExecutionReadCount(0).assertExecutionCloseCount(1).assertCapacity(0, 0);
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
    AsyncQueryScenario scenario = scenario().withTask(task).start().delete();

    scenario.assertDeleteStatus(PPLAsyncQueryService.Status.CANCELLED);
    verify(task).cancel("PPL asynchronous query cancelled by user");
    scenario.assertExecutionReadCount(0).assertExecutionCloseCount(1).assertCapacity(0, 0);
    assertThrows(ResourceNotFoundException.class, scenario::get);
  }

  @Test
  public void deleteReturnsExistingTerminalStatus() {
    CancellableTask task = mock(CancellableTask.class);
    AsyncQueryScenario scenario =
        scenario().withTask(task).withCurrentResult(response(1)).start().complete().delete();

    scenario.assertDeleteStatus(PPLAsyncQueryService.Status.SUCCEEDED);
    verify(task, never()).cancel(org.mockito.ArgumentMatchers.anyString());
    scenario.assertExecutionReadCount(0).assertExecutionCloseCount(1).assertCapacity(0, 0);
  }

  @Test
  public void cancellationUsesTaskManagerWhenAttached() {
    TaskManager taskManager = mock(TaskManager.class);
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    service.attachTaskManager(taskManager);
    scenario().withTask(task).start().delete();

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

    AsyncQueryScenario scenario =
        scenario()
            .start(request, registration.requestTask())
            .assertRetainedRunning(Optional.empty())
            .succeed(response(1));

    InOrder registrationOrder = inOrder(taskManager);
    registrationOrder
        .verify(taskManager)
        .registerChildNode(registration.requestTask().getId(), registration.localNode());
    registrationOrder.verify(taskManager).register("transport", PPLQueryAction.NAME, request);
    assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
    verify(taskManager).unregister(task);
    verify(registration.childNodeRegistration()).close();
    scenario.assertCapacity(0, 1);
  }

  @Test
  public void executionStartFailureCompletesJobAndReleasesTask() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    RequestTaskRegistration registration = registerRequestTask(taskManager, request, task);
    service.attachTaskManager(taskManager);

    AsyncQueryScenario scenario =
        scenario()
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .withExecutionStarter(
                ignored -> {
                  throw new IllegalStateException("execution did not start");
                })
            .start(request, registration.requestTask());

    scenario
        .assertFailure(IllegalStateException.class, "execution did not start")
        .assertCapacity(0, 0);
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
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

    scenario().start(request, registration.requestTask()).delete();

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
    AsyncQueryScenario scenario =
        scenario()
            .withService(abortingService)
            .withWaitForCompletion(TimeValue.timeValueSeconds(5));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class, () -> scenario.start(request, registration.requestTask()));

    assertEquals("scheduler unavailable", failure.getMessage());
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
    scenario.assertCapacity(0, 0);
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
    AsyncQueryScenario scenario = scenario().withWaitForCompletion(TimeValue.timeValueSeconds(5));

    assertThrows(IllegalStateException.class, () -> scenario.start(request, requestTask));

    verify(taskManager, never()).register("transport", PPLQueryAction.NAME, request);
    scenario.assertCapacity(0, 0);
  }

  @Test
  public void rejectsUnauthorizedCallerWithoutRenewingOrDeleting() {
    PPLAsyncQueryUser securedOwner = new PPLAsyncQueryUser("alice", "tenant", List.of("role-a"));
    PPLAsyncQueryUser otherUser = new PPLAsyncQueryUser("bob", "tenant", List.of("role-a"));
    AsyncQueryScenario scenario = scenario().withOwner(securedOwner).start();

    assertThrows(
        OpenSearchSecurityException.class, () -> service.get(scenario.id(), otherUser, null));
    assertThrows(OpenSearchSecurityException.class, () -> service.delete(scenario.id(), otherUser));
    assertEquals(
        PPLAsyncQueryService.Status.RUNNING,
        service.get(scenario.id(), securedOwner, null).status());
  }

  @Test
  public void enforcesRunningAndRetainedCapacity() {
    PPLAsyncQueryService limited = service(1, 1);
    scenario().withService(limited).start();

    OpenSearchStatusException exception =
        assertThrows(
            OpenSearchStatusException.class, () -> scenario().withService(limited).start());

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
    AsyncQueryScenario scenario = scenario().withService(missingOwnerNode);

    assertThrows(NullPointerException.class, scenario::start);

    scenario.assertCapacity(0, 0);
  }

  @Test
  public void finalSnapshotDefensivelyCopiesRows() {
    List<org.opensearch.sql.data.model.ExprValue> rows = new ArrayList<>();
    rows.add(ExprValueUtils.stringValue("first"));
    QueryResponse response =
        new QueryResponse(
            new Schema(List.of(new Column("state", null, ExprCoreType.STRING))), rows, null);

    AsyncQueryScenario scenario =
        scenario()
            .withCurrentResult(response)
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start()
            .complete();
    rows.add(ExprValueUtils.stringValue("second"));

    scenario.assertSucceededRowCount(1);
  }

  @Test
  public void completedExecutionCanBeAttachedBeforeCompletionIsObserved() {
    scenario()
        .succeed(response(2))
        .withWaitForCompletion(TimeValue.timeValueSeconds(5))
        .start()
        .assertDirectSuccess(response(2), 0)
        .assertExecutionReadOnceAndClosed();
  }

  @Test
  public void runningGetMaterializesCurrentResultOutsideJob() {
    AsyncQueryScenario scenario = scenario().withCurrentResult(response(1)).start().get();

    scenario.assertGetResponse(
        new PPLAsyncQueryService.JobSnapshot.Running(scenario.id(), Optional.of(response(1))));

    scenario
        .withCurrentResult(response(3))
        .get()
        .assertGetResponse(
            new PPLAsyncQueryService.JobSnapshot.Running(scenario.id(), Optional.of(response(3))));
  }

  @Test
  public void failedRetainedJobReturnsNoProvisionalRowsAndClosesExecution() {
    NumericMetric<Long> failures =
        new NumericMetric<>(MetricName.PPL_FAILED_REQ_COUNT_SYS.getName(), new BasicCounter());
    Metrics.getInstance().registerMetric(failures);
    try {
      AsyncQueryScenario scenario =
          scenario()
              .withCurrentResult(response(1))
              .start()
              .fail(new IllegalStateException("boom"))
              .get();

      scenario
          .assertGetResponse(
              new PPLAsyncQueryService.JobSnapshot.Failed(
                  Optional.of(scenario.id()),
                  new PPLAsyncQueryService.Failure("IllegalStateException", "boom"),
                  0))
          .assertExecutionReadCount(0)
          .assertExecutionCloseCount(1);
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
      scenario().start().fail(new IllegalArgumentException("invalid query"));

      assertEquals(Long.valueOf(1), failures.getValue());
    } finally {
      Metrics.getInstance().unregisterMetric(failures.getName());
    }
  }

  @Test
  public void deleteBeforeExecutionAttachmentClosesLateHandle() {
    TrackingExecution execution = new TrackingExecution(response(1));
    AsyncQueryScenario scenario = scenario().withExecution(execution);
    scenario
        .withExecutionStarter(
            ignored -> {
              scenario.delete();
              return execution;
            })
        .start();

    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, scenario::get);
  }

  @Test
  public void concurrentGetDoesNotBlockDeleteOnResultMaterialization() throws Exception {
    BlockingExecution execution = new BlockingExecution(response(1));
    AsyncQueryScenario scenario = scenario().withExecution(execution).start();

    CompletableFuture<PPLAsyncQueryService.JobSnapshot> get =
        CompletableFuture.supplyAsync(() -> service.get(scenario.id(), OWNER, null));
    assertTrue(execution.readStarted.await(5, TimeUnit.SECONDS));
    CompletableFuture<PPLAsyncQueryService.DeleteResult> delete =
        CompletableFuture.supplyAsync(() -> service.delete(scenario.id(), OWNER));

    try {
      assertEquals(PPLAsyncQueryService.Status.CANCELLED, delete.get(5, TimeUnit.SECONDS).status());
    } finally {
      execution.allowRead.countDown();
    }

    assertEquals(PPLAsyncQueryService.Status.RUNNING, get.get(5, TimeUnit.SECONDS).status());
    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, scenario::get);
  }

  @Test
  public void shutdownClosesRetainedExecutionExactlyOnce() throws Exception {
    AsyncQueryScenario scenario = scenario().withCurrentResult(response(1)).start();

    service.close();
    service.close();

    scenario.assertExecutionReadCount(0).assertExecutionCloseCount(1).assertCapacity(0, 0);
  }

  @Test
  public void successfulCompletionRequiresFinalResultToBeVisible() {
    scenario()
        .withWaitForCompletion(TimeValue.timeValueSeconds(5))
        .start()
        .complete()
        .assertFailure(IllegalStateException.class)
        .assertExecutionReadOnceAndClosed()
        .assertCapacity(0, 0);
  }

  @Test
  public void retentionResponseMaterializationFailureAbortsUndeliverableJob() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    ThrowingExecution execution = new ThrowingExecution();
    AsyncQueryScenario scenario =
        scenario()
            .withTask(task)
            .withExecution(execution)
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start()
            .fireTimeout();

    scenario.assertFailure(IllegalStateException.class).assertCapacity(0, 0);
    verify(task).cancel("PPL asynchronous query startup failed");
    assertEquals(1, execution.closes.get());
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

  private AsyncQueryScenario scenario() {
    return new AsyncQueryScenario();
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

  private static QueryResponse response(int rowCount) {
    Schema schema = new Schema(List.of(new Column("state", null, ExprCoreType.STRING)));
    return new QueryResponse(
        schema,
        java.util.stream.IntStream.range(0, rowCount)
            .mapToObj(i -> ExprValueUtils.stringValue("state-" + i))
            .toList(),
        null);
  }

  private final class AsyncQueryScenario {
    private PPLAsyncQueryService targetService = service;
    private PPLAsyncQueryUser owner = OWNER;
    private TimeValue keepAlive = KEEP_ALIVE;
    private TimeValue waitForCompletion = TimeValue.ZERO;
    private CancellableTask task;
    private TrackingExecution trackingExecution = new TrackingExecution(null);
    private AsyncQueryExecution execution = trackingExecution;
    private Function<CancellableTask, AsyncQueryExecution> executionStarter = ignored -> execution;
    private final AtomicReference<PPLAsyncQueryService.JobSnapshot> initialResponse =
        new AtomicReference<>();
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final AtomicInteger responseCount = new AtomicInteger();
    private PPLAsyncQueryService.JobSnapshot getResponse;
    private PPLAsyncQueryService.DeleteResult deleteResponse;

    private AsyncQueryScenario withService(PPLAsyncQueryService service) {
      targetService = service;
      return this;
    }

    private AsyncQueryScenario withOwner(PPLAsyncQueryUser owner) {
      this.owner = owner;
      return this;
    }

    private AsyncQueryScenario withKeepAlive(TimeValue keepAlive) {
      this.keepAlive = keepAlive;
      return this;
    }

    private AsyncQueryScenario withWaitForCompletion(TimeValue waitForCompletion) {
      this.waitForCompletion = waitForCompletion;
      return this;
    }

    private AsyncQueryScenario withTask(CancellableTask task) {
      this.task = task;
      return this;
    }

    private AsyncQueryScenario withCurrentResult(QueryResponse response) {
      trackingExecution.setCurrent(response);
      return this;
    }

    private AsyncQueryScenario withExecution(AsyncQueryExecution execution) {
      this.execution = execution;
      trackingExecution = execution instanceof TrackingExecution tracking ? tracking : null;
      executionStarter = ignored -> this.execution;
      return this;
    }

    private AsyncQueryScenario withExecutionStarter(
        Function<CancellableTask, AsyncQueryExecution> executionStarter) {
      this.executionStarter = executionStarter;
      return this;
    }

    private AsyncQueryScenario start() {
      targetService.start(
          owner, keepAlive, waitForCompletion, jobTask(task), executionStarter, responseListener());
      return this;
    }

    private AsyncQueryScenario start(TransportPPLQueryRequest request, PPLQueryTask requestTask) {
      targetService.start(
          owner,
          keepAlive,
          waitForCompletion,
          request,
          requestTask,
          executionStarter,
          responseListener());
      return this;
    }

    private ActionListener<PPLAsyncQueryService.JobSnapshot> responseListener() {
      return ActionListener.wrap(
          snapshot -> {
            initialResponse.set(snapshot);
            responseCount.incrementAndGet();
          },
          failure::set);
    }

    private AsyncQueryScenario completeAfterMillis(long elapsedMillis) {
      now.addAndGet(elapsedMillis);
      trackingExecution.complete();
      return this;
    }

    private AsyncQueryScenario succeedAfterMillis(long elapsedMillis, QueryResponse response) {
      now.addAndGet(elapsedMillis);
      trackingExecution.succeed(response);
      return this;
    }

    private AsyncQueryScenario complete() {
      trackingExecution.complete();
      return this;
    }

    private AsyncQueryScenario succeed(QueryResponse response) {
      trackingExecution.succeed(response);
      return this;
    }

    private AsyncQueryScenario fail(Exception failure) {
      trackingExecution.fail(failure);
      return this;
    }

    private AsyncQueryScenario advanceMillis(long elapsedMillis) {
      now.addAndGet(elapsedMillis);
      return this;
    }

    private AsyncQueryScenario reapExpired() {
      targetService.reapExpired();
      return this;
    }

    private AsyncQueryScenario assertDirectSuccess(
        QueryResponse expectedResponse, long expectedTookMillis) {
      assertEquals(
          new PPLAsyncQueryService.JobSnapshot.Succeeded(
              Optional.empty(), expectedResponse, expectedTookMillis),
          initialResponse.get());
      return this;
    }

    private AsyncQueryScenario assertSucceededRowCount(int expected) {
      assertTrue(initialResponse.get() instanceof PPLAsyncQueryService.JobSnapshot.Succeeded);
      PPLAsyncQueryService.JobSnapshot.Succeeded succeeded =
          (PPLAsyncQueryService.JobSnapshot.Succeeded) initialResponse.get();
      assertEquals(expected, succeeded.response().getResults().size());
      return this;
    }

    private AsyncQueryScenario assertRetainedRunning(Optional<QueryResponse> expectedResponse) {
      assertEquals(
          new PPLAsyncQueryService.JobSnapshot.Running(id(), expectedResponse),
          initialResponse.get());
      return this;
    }

    private AsyncQueryScenario assertGetResponse(
        PPLAsyncQueryService.JobSnapshot expectedResponse) {
      assertEquals(expectedResponse, getResponse);
      return this;
    }

    private AsyncQueryScenario assertNotRetained() {
      return assertCapacity(0, 0);
    }

    private AsyncQueryScenario assertCapacity(int running, int retained) {
      assertEquals(running, targetService.runningQueryCount());
      assertEquals(retained, targetService.retainedJobCount());
      return this;
    }

    private AsyncQueryScenario assertExecutionReadOnceAndClosed() {
      return assertExecutionReadCount(1).assertExecutionCloseCount(1);
    }

    private AsyncQueryScenario assertExecutionReadCount(int expected) {
      assertEquals(expected, trackingExecution.reads.get());
      return this;
    }

    private AsyncQueryScenario assertExecutionCloseCount(int expected) {
      assertEquals(expected, trackingExecution.closes.get());
      return this;
    }

    private AsyncQueryScenario assertTimeoutCancelled() {
      assertTrue(timeoutCancelled.get());
      return this;
    }

    private AsyncQueryScenario assertNoInitialResponse() {
      assertNull(initialResponse.get());
      return this;
    }

    private AsyncQueryScenario assertFailure(Class<? extends Exception> type, String message) {
      assertTrue(type.isInstance(failure.get()));
      assertEquals(message, failure.get().getMessage());
      return this;
    }

    private AsyncQueryScenario assertFailure(Class<? extends Exception> type) {
      assertTrue(type.isInstance(failure.get()));
      return this;
    }

    private AsyncQueryScenario fireTimeout() {
      timeoutTask.get().run();
      return this;
    }

    private AsyncQueryScenario assertResponseCount(int expected) {
      assertEquals(expected, responseCount.get());
      return this;
    }

    private AsyncQueryScenario get() {
      getResponse = targetService.get(id(), owner, null);
      return this;
    }

    private AsyncQueryScenario delete() {
      deleteResponse = targetService.delete(id(), owner);
      return this;
    }

    private AsyncQueryScenario assertDeleteStatus(PPLAsyncQueryService.Status status) {
      assertEquals(new PPLAsyncQueryService.DeleteResult(id(), status), deleteResponse);
      return this;
    }

    private String id() {
      return initialResponse.get().id().orElseThrow();
    }
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
