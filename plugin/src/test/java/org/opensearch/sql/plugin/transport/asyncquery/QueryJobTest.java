/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.data.model.ExprValue;
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
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskManager;

public class QueryJobTest {
  private static final String LOCAL_NODE = "node-a";
  private static final QueryJobOwner OWNER = QueryJobOwner.UNSECURED;
  private static final TimeValue KEEP_ALIVE = TimeValue.timeValueMinutes(5);
  private static final TimeValue DEFAULT_WAIT = TimeValue.timeValueSeconds(5);

  @Test
  public void fastSuccessReturnsDirectResultWithoutRetention() {
    scenario()
        .withWaitForCompletion(TimeValue.timeValueSeconds(5))
        .withCurrentResult(response(2))
        .start()
        .completeAfterMillis(25)
        .assertDirectSuccess(response(2), 25)
        .assertNotRegistered()
        .assertExecutionClosedOnce();
  }

  @Test
  public void completedExecutionCanBeAttachedBeforeCompletionIsObserved() {
    scenario()
        .succeed(response(2))
        .withWaitForCompletion(TimeValue.timeValueSeconds(5))
        .start()
        .assertDirectSuccess(response(2), 0)
        .assertExecutionClosedOnce();
  }

  @Test
  public void timeoutReturnsIdAndLaterGetReturnsCompleteResult() {
    QueryJobScenario scenario =
        scenario()
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start()
            .fireRetention()
            .assertRetainedRunning(Optional.empty());

    scenario
        .succeedAfterMillis(25, response(2))
        .get()
        .assertGetResponse(
            new QueryJob.Snapshot.Succeeded(Optional.of(scenario.id()), response(2), 25));
  }

  @Test
  public void fastFailureReturnsDirectFailureWithoutId() {
    scenario()
        .withWaitForCompletion(TimeValue.timeValueSeconds(5))
        .start()
        .fail(new IllegalStateException("boom"))
        .assertFailure(IllegalStateException.class, "boom")
        .assertNotRegistered()
        .assertExecutionClosedOnce();
  }

  @Test
  public void executionStartFailureCompletesJobAndReleasesTask() {
    QueryJobScenario scenario =
        scenario()
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .withExecutionStarter(
                ignored -> {
                  throw new IllegalStateException("execution did not start");
                });
    scenario.start();
    scenario
        .assertFailure(IllegalStateException.class, "execution did not start")
        .assertNotRegistered()
        .assertTaskClosed();
  }

  @Test
  public void retainedFailureRetainsFailureSnapshotAndRecordsMetric() {
    NumericMetric<Long> customerFailures =
        new NumericMetric<>(MetricName.PPL_FAILED_REQ_COUNT_CUS.getName(), new BasicCounter());
    Metrics.getInstance().registerMetric(customerFailures);
    try {
      QueryJobScenario scenario =
          scenario()
              .withWaitForCompletion(TimeValue.timeValueSeconds(5))
              .start()
              .fireRetention()
              .fail(new IllegalArgumentException("invalid query"))
              .get();

      scenario.assertGetResponse(
          new QueryJob.Snapshot.Failed(
              Optional.of(scenario.id()),
              new QueryJob.Failure("IllegalArgumentException", "invalid query"),
              0));
      assertEquals(Long.valueOf(1), customerFailures.getValue());
    } finally {
      Metrics.getInstance().unregisterMetric(customerFailures.getName());
    }
  }

  @Test
  public void expiredGetRemovesJobAndCancelsTask() {
    CancellableTask task = runningTask();
    QueryJobScenario scenario =
        scenario().withTask(task).start().fireRetention().advanceMillis(KEEP_ALIVE.millis());

    ResourceNotFoundException error = assertThrows(ResourceNotFoundException.class, scenario::get);

    assertEquals("PPL asynchronous query not found", error.getMessage());
    scenario.assertNotRegistered();
    verify(task).cancel("PPL asynchronous query expired");
  }

  @Test
  public void expiryTimerRemovesJobAfterKeepAliveElapses() {
    CancellableTask task = runningTask();
    QueryJobScenario scenario =
        scenario().withTask(task).start().fireRetention().assertRegistered();

    scenario.advanceMillis(KEEP_ALIVE.millis()).fireExpiry();

    scenario.assertNotRegistered();
    verify(task).cancel("PPL asynchronous query expired");
  }

  @Test
  public void deleteCancelsRunningJobAndReturnsCancelledStatus() {
    CancellableTask task = runningTask();
    QueryJobScenario scenario = scenario().withTask(task).start().fireRetention().delete();

    scenario.assertDeleteStatus(QueryJob.Status.CANCELLED);
    verify(task).cancel("PPL asynchronous query cancelled by user");
    scenario.assertNotRegistered().assertExecutionClosedOnce();
    assertThrows(ResourceNotFoundException.class, scenario::get);
  }

  @Test
  public void deleteReturnsRetainedTerminalStatus() {
    CancellableTask task = runningTask();
    QueryJobScenario scenario =
        scenario()
            .withTask(task)
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .withCurrentResult(response(1))
            .start()
            .fireRetention()
            .complete()
            .delete();

    scenario.assertDeleteStatus(QueryJob.Status.SUCCEEDED);
    verify(task, never()).cancel(org.mockito.ArgumentMatchers.anyString());
    scenario.assertNotRegistered().assertExecutionClosedOnce();
  }

  @Test
  public void deleteReturnsNotFoundAfterExpirationBoundary() {
    QueryJobScenario scenario = scenario().start().fireRetention();

    scenario.advanceMillis(KEEP_ALIVE.millis());

    assertThrows(ResourceNotFoundException.class, scenario::delete);
    scenario.assertNotRegistered();
  }

  @Test
  public void getRenewsLeaseAndResetsExpiryTimer() {
    QueryJobScenario scenario =
        scenario()
            .withKeepAlive(TimeValue.timeValueSeconds(1))
            .start()
            .fireRetention()
            .advanceMillis(500)
            .getWithKeepAlive(TimeValue.timeValueSeconds(2))
            .advanceMillis(1_999)
            .assertRegistered();

    // Original lease would have expired at 1000ms; renewed lease survives to 2499ms.
    scenario.advanceMillis(1).fireExpiry().assertNotRegistered();
  }

  @Test
  public void cancellationUsesTaskManagerWhenAttached() {
    TaskManager taskManager = mock(TaskManager.class);
    CancellableTask task = runningTask();
    scenario().withTaskManager(taskManager).withTask(task).start().fireRetention().delete();

    verify(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.eq("PPL asynchronous query cancelled by user"),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void shutdownDiscardsRunningJobAndClosesResources() {
    CancellableTask task = runningTask();
    QueryJobScenario scenario =
        scenario().withTask(task).withWaitForCompletion(TimeValue.timeValueSeconds(5)).start();

    scenario.closeRegistry();

    scenario.assertNotRegistered().assertExecutionClosedOnce();
    verify(task).cancel("PPL asynchronous query service is closing");
  }

  @Test
  public void unauthorizedCallerCannotAccessOwnedJob() {
    QueryJobOwner alice = new QueryJobOwner("alice", "tenant-a", List.of("role-a"));
    QueryJobOwner bob = new QueryJobOwner("bob", "tenant-a", List.of("role-a"));
    QueryJobScenario scenario = scenario().withOwner(alice).start().fireRetention();

    QueryJob job = scenario.job();
    assertThrows(OpenSearchSecurityException.class, () -> job.getOwner().authorize(bob));
    job.getOwner().authorize(alice);
  }

  @Test
  public void lateExecutionAttachmentAfterDeleteIsClosed() {
    TrackingExecution execution = new TrackingExecution(response(1));
    QueryJobScenario scenario = scenario().withExecution(execution);
    scenario
        .withExecutionStarter(
            ignored -> {
              // Retention fires and delete removes the job before the execution attaches.
              scenario.fireRetention();
              QueryJobId retainedId =
                  ((QueryJob.Snapshot.Running) scenario.initialResponse.get()).jobId();
              scenario.registry.get(retainedId).orElseThrow().cancel("test delete");
              return execution;
            })
        .start();

    assertEquals(1, execution.closes.get());
    assertTrue(scenario.registry.jobs().isEmpty());
  }

  @Test
  public void finalSnapshotDefensivelyCopiesRows() {
    List<ExprValue> rows = new ArrayList<>();
    rows.add(ExprValueUtils.stringValue("first"));
    QueryResponse response =
        new QueryResponse(
            new Schema(List.of(new Column("state", null, ExprCoreType.STRING))), rows, null);

    QueryJobScenario scenario =
        scenario()
            .withCurrentResult(response)
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start()
            .complete();
    rows.add(ExprValueUtils.stringValue("second"));

    scenario.assertSucceededRowCount(1);
  }

  @Test
  public void successfulCompletionRequiresFinalResultToBeVisible() {
    scenario()
        .withWaitForCompletion(TimeValue.timeValueSeconds(5))
        .start()
        .complete()
        .assertFailure(IllegalStateException.class)
        .assertExecutionClosedOnce();
  }

  @Test
  public void retentionListenerFailureAbortsUndeliverableJob() {
    CancellableTask task = runningTask();
    QueryJobScenario scenario =
        scenario()
            .withTask(task)
            .withResponseListener(new FailingListener())
            .withWaitForCompletion(TimeValue.timeValueSeconds(5))
            .start();

    scenario.fireRetention();

    scenario.assertNotRegistered().assertExecutionClosedOnce();
    verify(task).cancel("PPL asynchronous query startup failed");
  }

  private QueryJobScenario scenario() {
    return new QueryJobScenario();
  }

  private static CancellableTask runningTask() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    return task;
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

  /** Deterministic fluent scenario driving one {@link QueryJob} through its lifecycle. */
  private final class QueryJobScenario {
    private final AtomicLong clock = new AtomicLong(1_000);
    private final QueryJobRegistry registry = new QueryJobRegistry();
    private final FakeScheduler scheduler = new FakeScheduler();
    private QueryJobOwner owner = OWNER;
    private TimeValue keepAlive = KEEP_ALIVE;
    private TimeValue waitForCompletion = DEFAULT_WAIT;
    private CancellableTask task = mock(CancellableTask.class);
    private final AtomicInteger taskReleaseCount = new AtomicInteger();
    private TaskManager taskManager;
    private TrackingExecution trackingExecution = new TrackingExecution(null);
    private AsyncQueryExecution execution = trackingExecution;
    private Function<CancellableTask, AsyncQueryExecution> executionStarter = ignored -> execution;
    private ActionListener<QueryJob.Snapshot> responseListener;
    private final AtomicReference<QueryJob.Snapshot> initialResponse = new AtomicReference<>();
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final AtomicInteger responseCount = new AtomicInteger();
    private QueryJob.Snapshot getResponse;
    private QueryJob.Status deleteStatus;
    private QueryJob job;

    private QueryJobScenario withOwner(QueryJobOwner owner) {
      this.owner = owner;
      return this;
    }

    private QueryJobScenario withKeepAlive(TimeValue keepAlive) {
      this.keepAlive = keepAlive;
      return this;
    }

    private QueryJobScenario withWaitForCompletion(TimeValue wait) {
      this.waitForCompletion = wait;
      return this;
    }

    private QueryJobScenario withTask(CancellableTask task) {
      this.task = task;
      return this;
    }

    private QueryJobScenario withTaskManager(TaskManager taskManager) {
      this.taskManager = taskManager;
      return this;
    }

    private QueryJobScenario withCurrentResult(QueryResponse response) {
      trackingExecution.setCurrent(response);
      return this;
    }

    private QueryJobScenario withExecution(AsyncQueryExecution execution) {
      this.execution = execution;
      trackingExecution = execution instanceof TrackingExecution tracking ? tracking : null;
      executionStarter = ignored -> this.execution;
      return this;
    }

    private QueryJobScenario withExecutionStarter(
        Function<CancellableTask, AsyncQueryExecution> starter) {
      this.executionStarter = starter;
      return this;
    }

    private QueryJobScenario withResponseListener(ActionListener<QueryJob.Snapshot> listener) {
      this.responseListener = listener;
      return this;
    }

    private QueryJobScenario start() {
      QueryJob.JobTask jobTask = new QueryJob.JobTask(task, taskReleaseCount::incrementAndGet);
      ActionListener<QueryJob.Snapshot> listener =
          responseListener != null ? responseListener : defaultListener();
      job =
          QueryJob.create(
              LOCAL_NODE,
              registry,
              (LongSupplier) clock::get,
              scheduler,
              taskManager,
              jobTask,
              owner,
              keepAlive,
              waitForCompletion,
              executionStarter,
              listener);
      return this;
    }

    private ActionListener<QueryJob.Snapshot> defaultListener() {
      return ActionListener.wrap(
          snapshot -> {
            initialResponse.set(snapshot);
            responseCount.incrementAndGet();
          },
          failure::set);
    }

    private QueryJobScenario succeed(QueryResponse response) {
      trackingExecution.succeed(response);
      return this;
    }

    private QueryJobScenario complete() {
      trackingExecution.complete();
      return this;
    }

    private QueryJobScenario completeAfterMillis(long elapsedMillis) {
      clock.addAndGet(elapsedMillis);
      trackingExecution.complete();
      return this;
    }

    private QueryJobScenario succeedAfterMillis(long elapsedMillis, QueryResponse response) {
      clock.addAndGet(elapsedMillis);
      trackingExecution.succeed(response);
      return this;
    }

    private QueryJobScenario fail(Exception exception) {
      trackingExecution.fail(exception);
      return this;
    }

    private QueryJobScenario advanceMillis(long delta) {
      clock.addAndGet(delta);
      return this;
    }

    private QueryJobScenario fireRetention() {
      scheduler.fireNext();
      return this;
    }

    private QueryJobScenario fireExpiry() {
      scheduler.fireNext();
      return this;
    }

    private QueryJobScenario get() {
      getResponse = job.get(null);
      return this;
    }

    private QueryJobScenario getWithKeepAlive(TimeValue newKeepAlive) {
      getResponse = job.get(newKeepAlive);
      return this;
    }

    private QueryJobScenario delete() {
      deleteStatus = job.cancel("PPL asynchronous query cancelled by user");
      return this;
    }

    private QueryJobScenario closeRegistry() {
      registry.close();
      return this;
    }

    private QueryJobScenario assertDirectSuccess(QueryResponse expected, long tookMillis) {
      assertEquals(
          new QueryJob.Snapshot.Succeeded(Optional.empty(), expected, tookMillis),
          initialResponse.get());
      return this;
    }

    private QueryJobScenario assertRetainedRunning(Optional<QueryResponse> expected) {
      assertEquals(new QueryJob.Snapshot.Running(id(), expected), initialResponse.get());
      return this;
    }

    private QueryJobScenario assertGetResponse(QueryJob.Snapshot expected) {
      assertEquals(expected, getResponse);
      return this;
    }

    private QueryJobScenario assertDeleteStatus(QueryJob.Status expected) {
      assertEquals(expected, deleteStatus);
      return this;
    }

    private QueryJobScenario assertSucceededRowCount(int expected) {
      assertTrue(initialResponse.get() instanceof QueryJob.Snapshot.Succeeded);
      QueryJob.Snapshot.Succeeded succeeded = (QueryJob.Snapshot.Succeeded) initialResponse.get();
      assertEquals(expected, succeeded.response().getResults().size());
      return this;
    }

    private QueryJobScenario assertFailure(Class<? extends Exception> type) {
      assertTrue(type.isInstance(failure.get()));
      return this;
    }

    private QueryJobScenario assertFailure(Class<? extends Exception> type, String message) {
      assertTrue(type.isInstance(failure.get()));
      assertEquals(message, failure.get().getMessage());
      return this;
    }

    private QueryJobScenario assertRegistered() {
      assertTrue(registry.get(job.getJobId()).isPresent());
      return this;
    }

    private QueryJobScenario assertNotRegistered() {
      assertTrue(registry.get(job.getJobId()).isEmpty());
      return this;
    }

    private QueryJobScenario assertExecutionClosedOnce() {
      if (trackingExecution != null) {
        assertEquals(1, trackingExecution.closes.get());
      }
      return this;
    }

    private QueryJobScenario assertTaskClosed() {
      assertEquals(1, taskReleaseCount.get());
      return this;
    }

    private QueryJobId id() {
      return job.getJobId();
    }

    private QueryJob job() {
      return job;
    }
  }

  private static final class FakeScheduler implements QueryJob.Scheduler {
    private final Deque<Pending> pending = new ArrayDeque<>();

    @Override
    public QueryJob.Cancellable schedule(long delayMillis, Runnable task) {
      Pending entry = new Pending(delayMillis, task);
      pending.addLast(entry);
      return () -> pending.remove(entry);
    }

    void fireNext() {
      Pending entry = pending.pollFirst();
      assertTrue("No scheduled task", entry != null);
      entry.task.run();
    }

    private record Pending(long delayMillis, Runnable task) {}
  }

  private static final class TrackingExecution implements AsyncQueryExecution {
    private final AtomicReference<QueryResponse> current;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    final AtomicInteger reads = new AtomicInteger();
    final AtomicInteger closes = new AtomicInteger();

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

  private static final class FailingListener implements ActionListener<QueryJob.Snapshot> {
    @Override
    public void onResponse(QueryJob.Snapshot snapshot) {
      throw new IllegalStateException("listener rejected snapshot");
    }

    @Override
    public void onFailure(Exception e) {
      // no-op; QueryJob may deliver a subsequent failure once discard runs.
    }
  }
}
