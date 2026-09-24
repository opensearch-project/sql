/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.plugin.PPLQueryErrorHandler;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.PPLQueryTask;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.GetResult;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.JobTask;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Removal;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.ResponseContext;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Retention;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Transition;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * Owns the asynchronous PPL jobs assigned to the local node.
 *
 * <p>{@link PPLAsyncQueryJob} owns the mutable state of one job. This service owns the job registry
 * and performs the work requested by each state transition: capacity accounting, task management,
 * result materialization, listener notification, and execution cleanup. Those side effects happen
 * after the job lock is released.
 *
 * <p>POST races query completion against {@code wait_for_completion_timeout}. Completion wins by
 * returning the final result directly and removing the job. The timeout wins by retaining the job
 * and returning its opaque ID. GET and DELETE are then routed to this owner node.
 *
 * <p>This class is thread-safe. The registry is concurrent, capacity counters are guarded by {@code
 * admissionLock}, and each job synchronizes its own lifecycle transitions.
 */
public final class PPLAsyncQueryService extends AbstractLifecycleComponent {
  private static final Logger LOG = LogManager.getLogger(PPLAsyncQueryService.class);

  static final TimeValue DEFAULT_WAIT_FOR_COMPLETION =
      TimeValue.parseTimeValue(
          PPLQueryRequest.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT,
          PPLQueryRequest.WAIT_FOR_COMPLETION_TIMEOUT_FIELD);
  static final TimeValue DEFAULT_KEEP_ALIVE =
      TimeValue.parseTimeValue(
          PPLQueryRequest.DEFAULT_KEEP_ALIVE, PPLQueryRequest.KEEP_ALIVE_FIELD);
  private static final TimeValue REAPER_INTERVAL = TimeValue.timeValueMinutes(1);
  private static final TimeoutHandle NO_TIMEOUT = () -> {};

  /** Lifecycle state exposed in asynchronous PPL responses. */
  public enum Status {
    /** Query execution is still running. */
    RUNNING,

    /** Query execution completed successfully. */
    SUCCEEDED,

    /** Query execution failed. */
    FAILED,

    /** Query execution was cancelled by DELETE. */
    CANCELLED
  }

  @FunctionalInterface
  interface TimeoutHandle {
    void cancel();
  }

  @FunctionalInterface
  interface TimeoutScheduler {
    TimeoutHandle schedule(TimeValue delay, Runnable task);
  }

  /**
   * Immutable point-in-time response view of a job.
   *
   * <p>The formatter converts this internal model to the public JSON response. Query data is copied
   * from the execution after releasing the job lock, so formatting never observes mutable job
   * state.
   *
   * @param id opaque job ID, or {@code null} for a terminal response returned directly by POST
   * @param status lifecycle state captured with the result
   * @param response current query result, or {@code null} before a result is available
   * @param failure client-visible failure for {@link Status#FAILED}, otherwise {@code null}
   * @param tookMillis elapsed execution time, available for a completed job
   */
  public record JobSnapshot(
      String id, Status status, QueryResponse response, Failure failure, long tookMillis) {}

  /** Response model returned after DELETE removes a retained job. */
  record DeleteResult(String id, Status status) {}

  /**
   * Client-visible failure retained by a job and returned only after owner authorization.
   *
   * @param type exception type
   * @param reason client-facing failure reason
   */
  public record Failure(String type, String reason) {
    private static Failure from(Exception exception) {
      String type =
          exception.getClass().getSimpleName().isBlank()
              ? exception.getClass().getName()
              : exception.getClass().getSimpleName();
      String reason =
          exception.getMessage() == null || exception.getMessage().isBlank()
              ? "query execution failed"
              : exception.getMessage();
      return new Failure(type, reason);
    }
  }

  private final Supplier<String> ownerNodeIdSupplier;
  private final LongSupplier currentTimeMillis;
  private final TimeoutScheduler timeoutScheduler;
  private final IntSupplier maxRunningQueries;
  private final IntSupplier maxRetainedJobs;
  private final Supplier<TimeValue> maxWaitForCompletion;
  private final Supplier<TimeValue> maxKeepAlive;
  private final ThreadPool threadPool;
  private final ConcurrentMap<String, PPLAsyncQueryJob> jobs = new ConcurrentHashMap<>();
  private final Object admissionLock = new Object();

  private int runningQueries;
  private int retainedJobs;
  private volatile boolean acceptingNewJobs = true;
  private volatile Scheduler.Cancellable reaper;
  private volatile TaskManager taskManager;

  /**
   * Creates the owner-node lifecycle service.
   *
   * @param ownerNodeIdSupplier supplies the current local node ID
   * @param threadPool schedules retention deadlines and expiration reaping
   * @param settings supplies asynchronous query capacity and duration limits
   */
  public PPLAsyncQueryService(
      Supplier<String> ownerNodeIdSupplier, ThreadPool threadPool, Settings settings) {
    this(
        ownerNodeIdSupplier,
        System::currentTimeMillis,
        (delay, task) -> {
          Scheduler.ScheduledCancellable cancellable =
              threadPool.schedule(task, delay, ThreadPool.Names.GENERIC);
          return cancellable::cancel;
        },
        () ->
            (Integer)
                settings.getSettingValue(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES),
        () -> (Integer) settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS),
        () ->
            (TimeValue)
                settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_WAIT_FOR_COMPLETION_TIMEOUT),
        () -> (TimeValue) settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_KEEP_ALIVE),
        threadPool);
  }

  PPLAsyncQueryService(
      String ownerNodeId,
      LongSupplier currentTimeMillis,
      TimeoutScheduler timeoutScheduler,
      IntSupplier maxRunningQueries,
      IntSupplier maxRetainedJobs,
      Supplier<TimeValue> maxWaitForCompletion,
      Supplier<TimeValue> maxKeepAlive) {
    this(
        () -> ownerNodeId,
        currentTimeMillis,
        timeoutScheduler,
        maxRunningQueries,
        maxRetainedJobs,
        maxWaitForCompletion,
        maxKeepAlive,
        null);
  }

  private PPLAsyncQueryService(
      Supplier<String> ownerNodeIdSupplier,
      LongSupplier currentTimeMillis,
      TimeoutScheduler timeoutScheduler,
      IntSupplier maxRunningQueries,
      IntSupplier maxRetainedJobs,
      Supplier<TimeValue> maxWaitForCompletion,
      Supplier<TimeValue> maxKeepAlive,
      ThreadPool threadPool) {
    this.ownerNodeIdSupplier = Objects.requireNonNull(ownerNodeIdSupplier);
    this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis);
    this.timeoutScheduler = Objects.requireNonNull(timeoutScheduler);
    this.maxRunningQueries = Objects.requireNonNull(maxRunningQueries);
    this.maxRetainedJobs = Objects.requireNonNull(maxRetainedJobs);
    this.maxWaitForCompletion = Objects.requireNonNull(maxWaitForCompletion);
    this.maxKeepAlive = Objects.requireNonNull(maxKeepAlive);
    this.threadPool = threadPool;
  }

  /**
   * Starts an asynchronous PPL query and produces its POST response.
   *
   * <p>The service registers and owns a job task, schedules its retention deadline, starts
   * execution, and attaches the returned execution handle to the same job. If execution finishes
   * before the deadline, {@code responseListener} receives the final result without a job ID.
   * Otherwise, the job becomes retained and the listener receives its current result with an opaque
   * ID.
   *
   * @param owner submit caller retained with the job for later authorization
   * @param requestedKeepAlive requested job lease
   * @param requestedWaitForCompletion maximum time to wait for a direct result
   * @param request transport request used to register the job task
   * @param requestTask task associated with the POST request
   * @param executionStarter starts execution using the job-owned cancellable task
   * @param responseListener receives either the direct result or retained job ID
   */
  public void start(
      PPLAsyncQueryUser owner,
      String requestedKeepAlive,
      String requestedWaitForCompletion,
      TransportPPLQueryRequest request,
      PPLQueryTask requestTask,
      Function<CancellableTask, AsyncQueryExecution> executionStarter,
      ActionListener<JobSnapshot> responseListener) {
    TimeValue keepAlive =
        TimeValue.parseTimeValue(requestedKeepAlive, PPLQueryRequest.KEEP_ALIVE_FIELD);
    TimeValue waitForCompletion =
        TimeValue.parseTimeValue(
            requestedWaitForCompletion, PPLQueryRequest.WAIT_FOR_COMPLETION_TIMEOUT_FIELD);
    validateKeepAlive(keepAlive);
    validateWaitForCompletion(waitForCompletion);
    JobTask jobTask = registerJobTask(request, requestTask);
    start(owner, keepAlive, waitForCompletion, jobTask, executionStarter, responseListener);
  }

  /**
   * Starts a job using an already registered task.
   *
   * <p>This package-private entry point keeps task registration separate for tests while preserving
   * the same production lifecycle: create the job, establish its retention deadline, start
   * execution, and transfer ownership of the execution handle to the job.
   *
   * @param owner submit caller retained with the job for later authorization
   * @param keepAlive validated job lease
   * @param waitForCompletion validated direct-result wait
   * @param task job-owned task and registration cleanup
   * @param executionStarter starts execution using the job task
   * @param responseListener receives the POST response
   */
  void start(
      PPLAsyncQueryUser owner,
      TimeValue keepAlive,
      TimeValue waitForCompletion,
      JobTask task,
      Function<CancellableTask, AsyncQueryExecution> executionStarter,
      ActionListener<JobSnapshot> responseListener) {
    PPLAsyncQueryJob job;
    try {
      Objects.requireNonNull(task);
      Objects.requireNonNull(executionStarter);
      Objects.requireNonNull(responseListener);
      job = createJob(owner, keepAlive, task);
    } catch (RuntimeException | Error e) {
      closeTask(task);
      throw e;
    }

    TimeoutHandle retentionDeadline;
    try {
      retentionDeadline = scheduleRetention(job, waitForCompletion, responseListener);
    } catch (RuntimeException | Error e) {
      applyRemoval(job, job.abort());
      throw e;
    }
    startExecution(job, task.task(), executionStarter, responseListener, retentionDeadline);
  }

  private PPLAsyncQueryJob createJob(PPLAsyncQueryUser owner, TimeValue keepAlive, JobTask task) {
    Objects.requireNonNull(owner);
    reserveCapacity();
    try {
      String ownerNodeId =
          Objects.requireNonNull(ownerNodeIdSupplier.get(), "Local node ID is not initialized");
      long now = currentTimeMillis.getAsLong();
      while (true) {
        PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.create(ownerNodeId);
        String encodedId = jobId.encode();
        PPLAsyncQueryJob job =
            new PPLAsyncQueryJob(encodedId, owner, now, keepAlive.millis(), task);
        if (jobs.putIfAbsent(encodedId, job) == null) {
          return job;
        }
      }
    } catch (RuntimeException | Error e) {
      releaseCapacity();
      throw e;
    }
  }

  private TimeoutHandle scheduleRetention(
      PPLAsyncQueryJob job,
      TimeValue waitForCompletion,
      ActionListener<JobSnapshot> responseListener) {
    if (waitForCompletion.millis() == 0) {
      applyTransition(job, job.retain(currentTimeMillis.getAsLong()), responseListener);
      return NO_TIMEOUT;
    }

    return timeoutScheduler.schedule(
        waitForCompletion,
        () -> applyTransition(job, job.retain(currentTimeMillis.getAsLong()), responseListener));
  }

  private void startExecution(
      PPLAsyncQueryJob job,
      CancellableTask task,
      Function<CancellableTask, AsyncQueryExecution> executionStarter,
      ActionListener<JobSnapshot> responseListener,
      TimeoutHandle retentionDeadline) {
    try {
      AsyncQueryExecution execution = Objects.requireNonNull(executionStarter.apply(task));
      attachExecution(job, execution, responseListener, retentionDeadline);
    } catch (RuntimeException e) {
      retentionDeadline.cancel();
      fail(job, e, responseListener);
    }
  }

  private void attachExecution(
      PPLAsyncQueryJob job,
      AsyncQueryExecution execution,
      ActionListener<JobSnapshot> responseListener,
      TimeoutHandle retentionDeadline) {
    if (!job.tryAttachExecution(execution)) {
      retentionDeadline.cancel();
      closeExecution(execution);
      return;
    }
    execution
        .completion()
        .whenComplete(
            (ignored, failure) -> {
              retentionDeadline.cancel();
              if (failure == null) {
                complete(job, responseListener);
              } else {
                fail(job, asException(failure), responseListener);
              }
            });
  }

  private void complete(PPLAsyncQueryJob job, ActionListener<JobSnapshot> responseListener) {
    applyTransition(job, job.complete(currentTimeMillis.getAsLong()), responseListener);
  }

  private void fail(
      PPLAsyncQueryJob job, Exception failure, ActionListener<JobSnapshot> responseListener) {
    Objects.requireNonNull(failure);
    Transition transition = job.fail(Failure.from(failure), currentTimeMillis.getAsLong());
    if (transition == null) {
      return;
    }
    if (transition.response() == null) {
      PPLQueryErrorHandler.recordFailure(failure);
      applyTransition(job, transition, responseListener);
      return;
    }
    applyTransition(
        job,
        transition,
        ActionListener.wrap(
            ignored -> responseListener.onFailure(failure), responseListener::onFailure));
  }

  /**
   * Returns the current snapshot of a retained job.
   *
   * <p>The service authorizes the caller against the immutable job owner before requesting a
   * synchronized lease transition. Result materialization happens afterward and therefore cannot
   * block lifecycle transitions.
   *
   * @param id opaque job ID owned by this node
   * @param caller caller to compare with the stored job owner
   * @param requestedKeepAlive new lease duration, or {@code null} to leave the lease unchanged
   * @return immutable current job snapshot
   */
  JobSnapshot get(String id, PPLAsyncQueryUser caller, TimeValue requestedKeepAlive) {
    if (requestedKeepAlive != null) {
      validateKeepAlive(requestedKeepAlive);
    }
    PPLAsyncQueryJob job = findLocal(id);
    job.owner().authorize(caller);
    GetResult result = job.get(currentTimeMillis.getAsLong(), requestedKeepAlive);
    if (result instanceof GetResult.Expired expired) {
      applyRemoval(job, expired.removal());
      throw notFound();
    }
    return materialize(((GetResult.Found) result).response());
  }

  /**
   * Cancels and removes a retained job.
   *
   * @param id opaque job ID owned by this node
   * @param caller caller to compare with the stored job owner
   * @return the status observed when the job was removed
   */
  DeleteResult delete(String id, PPLAsyncQueryUser caller) {
    PPLAsyncQueryJob job = findLocal(id);
    job.owner().authorize(caller);
    Removal removal = job.delete(currentTimeMillis.getAsLong());
    applyRemoval(job, removal);
    if (removal.expired()) {
      throw notFound();
    }
    return new DeleteResult(id, removal.responseStatus());
  }

  void reapExpired() {
    long now = currentTimeMillis.getAsLong();
    jobs.forEach((id, job) -> applyRemoval(job, job.expire(now)));
  }

  int runningQueryCount() {
    synchronized (admissionLock) {
      return runningQueries;
    }
  }

  int retainedJobCount() {
    synchronized (admissionLock) {
      return retainedJobs;
    }
  }

  /**
   * Attaches the node task manager after transport actions have been initialized.
   *
   * @param taskManager task manager used to register and cancel retained query tasks
   */
  public void attachTaskManager(TaskManager taskManager) {
    this.taskManager = Objects.requireNonNull(taskManager);
  }

  private void reserveCapacity() {
    synchronized (admissionLock) {
      if (!acceptingNewJobs) {
        throw new OpenSearchStatusException(
            "PPL asynchronous query service is stopping", RestStatus.SERVICE_UNAVAILABLE);
      }
      if (runningQueries >= maxRunningQueries.getAsInt()
          || retainedJobs >= maxRetainedJobs.getAsInt()) {
        throw new OpenSearchStatusException(
            "PPL asynchronous query capacity is exhausted", RestStatus.TOO_MANY_REQUESTS);
      }
      runningQueries++;
      retainedJobs++;
    }
  }

  private void releaseRunning() {
    synchronized (admissionLock) {
      if (runningQueries > 0) {
        runningQueries--;
      }
    }
  }

  private void releaseRetained() {
    synchronized (admissionLock) {
      if (retainedJobs > 0) {
        retainedJobs--;
      }
    }
  }

  private void releaseCapacity() {
    synchronized (admissionLock) {
      if (runningQueries > 0) {
        runningQueries--;
      }
      if (retainedJobs > 0) {
        retainedJobs--;
      }
    }
  }

  /**
   * Applies side effects selected by a {@link PPLAsyncQueryJob} transition.
   *
   * <p>The job decides its state change while holding the job lock, then returns a value describing
   * the required side effects. Map mutation, capacity accounting, and listener callbacks happen
   * here after the lock has been released.
   */
  private void applyTransition(
      PPLAsyncQueryJob job, Transition transition, ActionListener<JobSnapshot> responseListener) {
    if (transition == null) {
      return;
    }
    if (transition.releasesRunningSlot()) {
      releaseRunning();
    }
    if (transition.retention() == Retention.REMOVE && jobs.remove(job.id(), job)) {
      releaseRetained();
    }

    JobSnapshot snapshot = null;
    RuntimeException materializationFailure = null;
    try {
      if (transition.response() != null) {
        snapshot = materialize(transition.response());
      }
    } catch (RuntimeException e) {
      materializationFailure = e;
    } finally {
      closeExecution(transition.executionToClose());
      closeTask(transition.taskToClose());
    }

    if (transition.response() != null) {
      if (materializationFailure == null) {
        responseListener.onResponse(snapshot);
      } else {
        if (transition.retention() == Retention.RETAIN) {
          applyRemoval(job, job.abort());
        }
        responseListener.onFailure(materializationFailure);
      }
    }
  }

  private void applyRemoval(PPLAsyncQueryJob job, Removal removal) {
    if (removal == null) {
      return;
    }
    if (jobs.remove(job.id(), job)) {
      if (removal.releasesRunningSlot()) {
        releaseRunning();
      }
      releaseRetained();
    }
    cancel(removal.task(), removal.reason());
    closeExecution(removal.execution());
  }

  private JobSnapshot materialize(ResponseContext context) {
    QueryResponse response = null;
    if (context.status() == Status.SUCCEEDED || context.status() == Status.RUNNING) {
      response =
          context.execution() == null
              ? null
              : context
                  .execution()
                  .currentResult()
                  .map(PPLAsyncQueryService::snapshotResponse)
                  .orElse(null);
    }
    if (context.status() == Status.SUCCEEDED && response == null) {
      throw new IllegalStateException(
          "Successful PPL asynchronous execution completed without a final result");
    }
    return new JobSnapshot(
        context.id(), context.status(), response, context.failure(), context.tookMillis());
  }

  private static void closeExecution(AsyncQueryExecution execution) {
    if (execution == null) {
      return;
    }
    try {
      execution.close();
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to close PPL asynchronous query execution ({})", e.getClass().getSimpleName());
    }
  }

  private static void closeTask(JobTask task) {
    if (task != null) {
      task.close();
    }
  }

  private PPLAsyncQueryJob findLocal(String encodedId) {
    PPLAsyncQueryJob job = jobs.get(encodedId);
    if (job == null) {
      throw notFound();
    }
    return job;
  }

  void validateKeepAlive(TimeValue keepAlive) {
    TimeValue maximum = maxKeepAlive.get();
    if (keepAlive == null || keepAlive.millis() <= 0 || keepAlive.millis() > maximum.millis()) {
      throw new IllegalArgumentException(
          "[keep_alive] must be greater than 0 and no more than " + maximum);
    }
  }

  void validateWaitForCompletion(TimeValue waitForCompletion) {
    TimeValue maximum = maxWaitForCompletion.get();
    if (waitForCompletion == null
        || waitForCompletion.millis() < 0
        || waitForCompletion.millis() > maximum.millis()) {
      throw new IllegalArgumentException(
          "[wait_for_completion_timeout] must be between 0 and " + maximum);
    }
  }

  /**
   * Copies an execution-owned response before exposing it through a job snapshot.
   *
   * <p>This prevents later execution updates from changing a response already handed to a caller.
   */
  private static QueryResponse snapshotResponse(QueryResponse response) {
    Schema schema = new Schema(List.copyOf(response.getSchema().getColumns()));
    QueryResponse copy =
        new QueryResponse(schema, List.copyOf(response.getResults()), response.getCursor());
    copy.setWarnings(List.copyOf(response.getWarnings()));
    return copy;
  }

  private void cancel(JobTask task, String reason) {
    if (task == null) {
      return;
    }
    CancellableTask cancellableTask = task.task();
    if (cancellableTask == null || cancellableTask.isCancelled()) {
      task.close();
      return;
    }
    try {
      TaskManager currentTaskManager = taskManager;
      if (currentTaskManager == null) {
        cancellableTask.cancel(reason);
        task.close();
      } else {
        currentTaskManager.cancelTaskAndDescendants(
            cancellableTask,
            reason,
            false,
            ActionListener.wrap(
                ignored -> task.close(),
                failure -> {
                  task.close();
                  LOG.warn(
                      "Failed to cancel descendants of PPL asynchronous query task ({})",
                      failure.getClass().getSimpleName());
                }));
      }
    } catch (RuntimeException e) {
      task.close();
      LOG.warn("Failed to cancel PPL asynchronous query task ({})", e.getClass().getSimpleName());
    }
  }

  /**
   * Registers the independently cancellable task owned by an asynchronous job.
   *
   * <p>The POST request task is assigned as parent during registration so cancellation can reach
   * startup work. The returned {@link JobTask} owns both task unregistration and child-node
   * registration cleanup.
   *
   * @param request request used by {@link TaskManager} to create the job task
   * @param requestTask task associated with the POST request
   * @return job-owned task and its registration cleanup
   */
  private JobTask registerJobTask(TransportPPLQueryRequest request, PPLQueryTask requestTask) {
    TaskManager currentTaskManager =
        Objects.requireNonNull(
            taskManager, "PPL asynchronous query task manager is not initialized");
    Objects.requireNonNull(requestTask, "PPL asynchronous query request task is not initialized");
    DiscoveryNode localNode =
        Objects.requireNonNull(currentTaskManager.localNode(), "Local node is not initialized");

    // This task is registered directly rather than through TransportAction.execute(), so reproduce
    // the two pieces of OpenSearch child-task bookkeeping that TransportAction normally performs.
    // The child-node registration lets parent cancellation send a ban to this node; parentTaskId
    // lets that ban find and cancel the retained task.
    Releasable childNodeRegistration =
        currentTaskManager.registerChildNode(requestTask.getId(), localNode);
    TaskId originalParent = request.getParentTask();
    boolean registered = false;
    try {
      request.setParentTask(localNode.getId(), requestTask.getId());
      Task task = currentTaskManager.register("transport", PPLQueryAction.NAME, request);
      if (!(task instanceof PPLQueryTask pplQueryTask)) {
        currentTaskManager.unregister(task);
        throw new IllegalStateException("Failed to create PPL asynchronous query task");
      }
      registered = true;
      return new JobTask(
          pplQueryTask,
          () -> {
            try {
              currentTaskManager.unregister(pplQueryTask);
            } finally {
              childNodeRegistration.close();
            }
          });
    } finally {
      request.setParentTask(originalParent);
      if (!registered) {
        childNodeRegistration.close();
      }
    }
  }

  private static Exception asException(Throwable failure) {
    Throwable cause =
        failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
    return cause instanceof Exception exception ? exception : new RuntimeException(cause);
  }

  private static ResourceNotFoundException notFound() {
    return new ResourceNotFoundException("PPL asynchronous query not found");
  }

  /** Starts accepting submissions and schedules periodic retained-job expiration. */
  @Override
  protected void doStart() {
    acceptingNewJobs = true;
    if (threadPool != null) {
      reaper =
          threadPool.scheduleWithFixedDelay(
              this::reapExpired, REAPER_INTERVAL, ThreadPool.Names.GENERIC);
    }
  }

  /** Stops accepting submissions and cancels the periodic expiration task. */
  @Override
  protected void doStop() {
    acceptingNewJobs = false;
    Scheduler.Cancellable scheduledReaper = reaper;
    if (scheduledReaper != null) {
      scheduledReaper.cancel();
      reaper = null;
    }
  }

  /**
   * Removes all remaining jobs and releases their task and execution resources.
   *
   * @throws IOException if lifecycle shutdown fails
   */
  @Override
  protected void doClose() throws IOException {
    acceptingNewJobs = false;
    jobs.forEach(
        (id, job) -> applyRemoval(job, job.close("PPL asynchronous query service is closing")));
  }
}
