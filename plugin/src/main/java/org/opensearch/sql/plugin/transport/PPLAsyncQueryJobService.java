/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.UpdateMode;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * Owner-node job store for Calcite PPL asynchronous execution.
 *
 * <p>The store owns the public lifecycle contract: authorization, lease renewal, current result
 * state, submit-time completion waiters, final paging state, cancellation, and expiration.
 */
public class PPLAsyncQueryJobService {
  private static final Logger LOG = LogManager.getLogger(PPLAsyncQueryJobService.class);

  static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
  static final TimeValue MAX_KEEP_ALIVE = TimeValue.timeValueHours(24);
  static final TimeValue DEFAULT_WAIT_FOR_COMPLETION = TimeValue.timeValueSeconds(5);
  static final TimeValue MAX_WAIT_FOR_COMPLETION = TimeValue.timeValueSeconds(60);
  static final int DEFAULT_PAGE_SIZE = 1_000;
  static final int MAX_JOBS = 10_000;

  enum Status {
    RUNNING,
    SUCCEEDED,
    FAILED,
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

  private final Supplier<String> ownerNodeIdSupplier;
  private final LongSupplier currentTimeMillis;
  private final TimeoutScheduler timeoutScheduler;
  private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

  public PPLAsyncQueryJobService(Supplier<String> ownerNodeIdSupplier, ThreadPool threadPool) {
    this(
        ownerNodeIdSupplier,
        System::currentTimeMillis,
        (delay, task) -> {
          Scheduler.ScheduledCancellable cancellable =
              threadPool.schedule(task, delay, ThreadPool.Names.GENERIC);
          return cancellable::cancel;
        });
  }

  PPLAsyncQueryJobService(
      String ownerNodeId, LongSupplier currentTimeMillis, TimeoutScheduler timeoutScheduler) {
    this(() -> ownerNodeId, currentTimeMillis, timeoutScheduler);
  }

  private PPLAsyncQueryJobService(
      Supplier<String> ownerNodeIdSupplier,
      LongSupplier currentTimeMillis,
      TimeoutScheduler timeoutScheduler) {
    this.ownerNodeIdSupplier = Objects.requireNonNull(ownerNodeIdSupplier);
    this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis);
    this.timeoutScheduler = Objects.requireNonNull(timeoutScheduler);
  }

  String create(User user, TimeValue keepAlive, CancellableTask task) {
    reapExpired();
    String ownerNodeId = ownerNodeId();
    if (jobs.size() >= MAX_JOBS) {
      throw new IllegalStateException(
          "Too many active PPL asynchronous jobs on node [" + ownerNodeId + "]");
    }
    validateKeepAlive(keepAlive);
    PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.create(ownerNodeId);
    String encoded = jobId.encode();
    long startTimeMillis = currentTimeMillis.getAsLong();
    jobs.put(jobId.contextId(), new Job(encoded, user, startTimeMillis, keepAlive.millis(), task));
    return encoded;
  }

  void classify(String id, UpdateMode updateMode) {
    Job job = findInternal(id);
    if (job != null) {
      publish(job.classify(updateMode));
    }
  }

  void progress(String id, QueryProgress progress) {
    Job job = findInternal(id);
    if (job != null) {
      publish(job.progress(progress));
    }
  }

  void partial(String id, QueryResponse response) {
    partial(id, response, null);
  }

  void partial(String id, QueryResponse response, QueryProgress progress) {
    Job job = findInternal(id);
    if (job != null) {
      publish(job.partial(copy(response, false), progress));
    }
  }

  void complete(String id, QueryResponse response) {
    Job job = findInternal(id);
    if (job != null) {
      publish(job.complete(copy(response, true), currentTimeMillis.getAsLong()));
    }
  }

  void fail(String id, Exception exception) {
    Job job = findInternal(id);
    if (job != null) {
      publish(job.fail(exception, currentTimeMillis.getAsLong()));
    }
  }

  void searchTaskStarted(String id, long operationId, Runnable cancelAction) {
    Job job = findInternal(id);
    if (job != null) {
      runCancellation(job.searchTaskStarted(operationId, cancelAction));
    }
  }

  void searchTaskFinished(String id, long operationId) {
    Job job = findInternal(id);
    if (job != null) {
      job.searchTaskFinished(operationId);
    }
  }

  Snapshot get(String id, User user, TimeValue keepAlive) {
    Job job = findAuthorized(id, user);
    return job.renewAndSnapshot(currentTimeMillis.getAsLong(), keepAlive);
  }

  void awaitCompletion(
      String id, User user, TimeValue waitTimeout, ActionListener<Snapshot> listener) {
    validateWaitTimeout(waitTimeout);
    Job job = findAuthorized(id, user);
    WaitRegistration registration =
        job.registerCompletionWaiter(currentTimeMillis.getAsLong(), null, waitTimeout, listener);
    finishRegistration(job, registration);
  }

  Snapshot removeSuccessfulFastPath(String id, User user) {
    PPLAsyncQueryJobId jobId = localId(id);
    Job job = jobs.get(jobId.contextId());
    if (job == null) {
      throw unknown(id);
    }
    job.authorize(user);
    Snapshot snapshot = job.snapshot();
    if (snapshot.status() == Status.SUCCEEDED) {
      jobs.remove(jobId.contextId(), job);
    }
    return snapshot;
  }

  Snapshot cancelAndRemove(String id, User user) {
    PPLAsyncQueryJobId jobId = localId(id);
    Job job = jobs.get(jobId.contextId());
    if (job == null) {
      throw unknown(id);
    }
    job.authorize(user);
    Publication publication =
        job.cancel("PPL asynchronous job cancelled by user", currentTimeMillis.getAsLong());
    publish(publication);
    jobs.remove(jobId.contextId(), job);
    runCancellations(publication.cancellations());
    return publication.snapshot();
  }

  public void reapExpired() {
    long now = currentTimeMillis.getAsLong();
    jobs.forEach(
        (contextId, job) -> {
          Expiration expiration = job.expireIfNeeded(now);
          if (expiration.expired() && jobs.remove(contextId, job)) {
            expiration
                .waiters()
                .forEach(
                    waiter ->
                        waiter.fail(
                            new ResourceNotFoundException(
                                "Expired PPL job id [" + job.id() + "]")));
            runCancellations(expiration.cancellations());
          }
        });
  }

  private void finishRegistration(Job job, WaitRegistration registration) {
    if (registration.snapshot() != null) {
      registration.listener().onResponse(registration.snapshot());
      return;
    }
    Waiter waiter = registration.waiter();
    try {
      TimeoutHandle timeout =
          timeoutScheduler.schedule(
              registration.timeout(),
              () -> {
                Snapshot snapshot = job.timeout(waiter);
                if (snapshot != null) {
                  waiter.respond(snapshot);
                }
              });
      waiter.setTimeout(timeout);
    } catch (Exception e) {
      job.removeWaiter(waiter);
      waiter.fail(e);
    }
  }

  private Job findAuthorized(String id, User user) {
    PPLAsyncQueryJobId jobId = localId(id);
    Job job = jobs.get(jobId.contextId());
    if (job == null) {
      throw unknown(id);
    }
    long now = currentTimeMillis.getAsLong();
    if (job.isExpired(now)) {
      Expiration expiration = job.expireIfNeeded(now);
      if (jobs.remove(jobId.contextId(), job)) {
        runCancellations(expiration.cancellations());
      }
      throw new ResourceNotFoundException("Expired PPL job id [" + id + "]");
    }
    job.authorize(user);
    return job;
  }

  private Job findInternal(String id) {
    PPLAsyncQueryJobId jobId = localId(id);
    Job job = jobs.get(jobId.contextId());
    if (job != null) {
      long now = currentTimeMillis.getAsLong();
      if (job.isExpired(now)) {
        Expiration expiration = job.expireIfNeeded(now);
        if (jobs.remove(jobId.contextId(), job)) {
          runCancellations(expiration.cancellations());
        }
        return null;
      }
    }
    return job;
  }

  private PPLAsyncQueryJobId localId(String id) {
    PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.parse(id);
    String ownerNodeId = ownerNodeId();
    if (!ownerNodeId.equals(jobId.ownerNodeId())) {
      throw new IllegalArgumentException(
          "PPL job id is owned by node ["
              + jobId.ownerNodeId()
              + "], not local node ["
              + ownerNodeId
              + "]");
    }
    return jobId;
  }

  private String ownerNodeId() {
    return Objects.requireNonNull(ownerNodeIdSupplier.get(), "Local node ID is not initialized");
  }

  static void validateKeepAlive(TimeValue keepAlive) {
    if (keepAlive == null
        || keepAlive.millis() <= 0
        || keepAlive.millis() > MAX_KEEP_ALIVE.millis()) {
      throw new IllegalArgumentException(
          "[keep_alive] must be greater than 0 and no more than " + MAX_KEEP_ALIVE);
    }
  }

  static void validateWaitTimeout(TimeValue timeout) {
    if (timeout == null
        || timeout.millis() < 0
        || timeout.millis() > MAX_WAIT_FOR_COMPLETION.millis()) {
      throw new IllegalArgumentException(
          "[wait_for_completion_timeout] must be between 0 and " + MAX_WAIT_FOR_COMPLETION);
    }
  }

  private static ResourceNotFoundException unknown(String id) {
    return new ResourceNotFoundException("Unknown PPL job id [" + id + "]");
  }

  private static QueryResponse copy(QueryResponse response, boolean includeCursor) {
    QueryResponse copy =
        new QueryResponse(
            response.getSchema(),
            List.copyOf(response.getResults()),
            includeCursor ? response.getCursor() : null);
    copy.setWarnings(List.copyOf(response.getWarnings()));
    copy.setProfile(response.getProfile());
    copy.setError(response.getError());
    return copy;
  }

  private static void publish(Publication publication) {
    if (publication != null) {
      publication.waiters().forEach(waiter -> waiter.respond(publication.snapshot()));
    }
  }

  private static void runCancellation(Runnable cancellation) {
    if (cancellation == null) {
      return;
    }
    try {
      cancellation.run();
    } catch (RuntimeException e) {
      LOG.warn("Failed to cancel a PPL asynchronous query task", e);
    }
  }

  private static void runCancellations(List<Runnable> cancellations) {
    cancellations.forEach(PPLAsyncQueryJobService::runCancellation);
  }

  record Snapshot(
      String id,
      Status status,
      boolean classified,
      UpdateMode updateMode,
      QueryProgress progress,
      QueryResponse response,
      Exception failure,
      long startTimeMillis,
      long expirationTimeMillis,
      long tookMillis) {}

  private record Publication(
      Snapshot snapshot, List<Waiter> waiters, List<Runnable> cancellations) {
    private Publication(Snapshot snapshot, List<Waiter> waiters) {
      this(snapshot, waiters, List.of());
    }
  }

  private record Expiration(boolean expired, List<Waiter> waiters, List<Runnable> cancellations) {
    private static final Expiration NOT_EXPIRED = new Expiration(false, List.of(), List.of());
  }

  private record WaitRegistration(
      Snapshot snapshot, Waiter waiter, ActionListener<Snapshot> listener, TimeValue timeout) {
    private static WaitRegistration immediate(
        Snapshot snapshot, ActionListener<Snapshot> listener) {
      return new WaitRegistration(snapshot, null, listener, TimeValue.ZERO);
    }

    private static WaitRegistration waiting(Waiter waiter, TimeValue timeout) {
      return new WaitRegistration(null, waiter, waiter.listener(), timeout);
    }
  }

  private static final class Waiter {
    private final ActionListener<Snapshot> listener;
    private final AtomicBoolean completed = new AtomicBoolean();
    private volatile TimeoutHandle timeout;

    private Waiter(ActionListener<Snapshot> listener) {
      this.listener = listener;
    }

    private boolean shouldComplete(Snapshot snapshot) {
      return snapshot.status() != Status.RUNNING;
    }

    private ActionListener<Snapshot> listener() {
      return listener;
    }

    private void setTimeout(TimeoutHandle timeout) {
      this.timeout = timeout;
      if (completed.get()) {
        timeout.cancel();
      }
    }

    private void respond(Snapshot snapshot) {
      if (completed.compareAndSet(false, true)) {
        TimeoutHandle scheduled = timeout;
        if (scheduled != null) {
          scheduled.cancel();
        }
        listener.onResponse(snapshot);
      }
    }

    private void fail(Exception e) {
      if (completed.compareAndSet(false, true)) {
        TimeoutHandle scheduled = timeout;
        if (scheduled != null) {
          scheduled.cancel();
        }
        listener.onFailure(e);
      }
    }
  }

  private static final class Job {
    private final String id;
    private final List<String> submitterBackendRoles;
    private final long startTimeMillis;
    private final CancellableTask parentTask;
    private final Map<Long, Runnable> activeSearchCancellations = new HashMap<>();
    private final List<Waiter> waiters = new ArrayList<>();

    private long keepAliveMillis;
    private long expirationTimeMillis;
    private Status status = Status.RUNNING;
    private UpdateMode updateMode = UpdateMode.REPLACE;
    private boolean classified;
    private QueryProgress progress = QueryProgress.ZERO;
    private QueryResponse response;
    private Exception failure;
    private long completionTimeMillis = -1L;

    private Job(
        String id,
        User user,
        long startTimeMillis,
        long keepAliveMillis,
        CancellableTask parentTask) {
      this.id = id;
      this.submitterBackendRoles =
          user == null || user.getBackendRoles() == null
              ? List.of()
              : List.copyOf(user.getBackendRoles());
      this.startTimeMillis = startTimeMillis;
      this.keepAliveMillis = keepAliveMillis;
      this.expirationTimeMillis = startTimeMillis + keepAliveMillis;
      this.parentTask = parentTask;
    }

    private String id() {
      return id;
    }

    private synchronized void authorize(User requestingUser) {
      List<String> requestingBackendRoles =
          requestingUser == null || requestingUser.getBackendRoles() == null
              ? List.of()
              : requestingUser.getBackendRoles();
      if (!requestingBackendRoles.containsAll(submitterBackendRoles)) {
        throw new OpenSearchSecurityException(
            "User does not have the backend roles required to access PPL job id [" + id + "]");
      }
    }

    private synchronized Publication classify(UpdateMode updateMode) {
      if (status != Status.RUNNING) {
        return null;
      }
      if (classified && this.updateMode != updateMode) {
        throw new IllegalStateException("PPL asynchronous job update mode cannot change");
      }
      this.updateMode = Objects.requireNonNull(updateMode);
      classified = true;
      return publication();
    }

    private synchronized Publication progress(QueryProgress progress) {
      if (status != Status.RUNNING) {
        return null;
      }
      QueryProgress next = runningProgress(progress);
      if (Objects.equals(this.progress, next)) {
        return null;
      }
      this.progress = next;
      return publication();
    }

    private synchronized Publication partial(QueryResponse response, QueryProgress progress) {
      if (status != Status.RUNNING) {
        return null;
      }
      if (!classified) {
        throw new IllegalStateException("The PPL asynchronous job has not been classified");
      }
      if (updateMode == UpdateMode.APPEND
          && this.response != null
          && response.getResults().size() < this.response.getResults().size()) {
        throw new IllegalStateException("APPEND result size cannot decrease");
      }
      this.response = response;
      if (progress != null) {
        this.progress = runningProgress(progress);
      }
      return publication();
    }

    private synchronized Publication complete(QueryResponse response, long now) {
      if (status != Status.RUNNING) {
        return null;
      }
      this.response = response;
      progress = new QueryProgress(1D);
      status = Status.SUCCEEDED;
      completionTimeMillis = now;
      activeSearchCancellations.clear();
      return publication();
    }

    private synchronized Publication fail(Exception failure, long now) {
      if (status != Status.RUNNING) {
        return null;
      }
      this.failure = failure;
      status = Status.FAILED;
      completionTimeMillis = now;
      activeSearchCancellations.clear();
      return publication();
    }

    private synchronized Runnable searchTaskStarted(long operationId, Runnable cancelAction) {
      if (status != Status.RUNNING) {
        return cancelAction;
      }
      activeSearchCancellations.put(operationId, cancelAction);
      return null;
    }

    private synchronized void searchTaskFinished(long operationId) {
      activeSearchCancellations.remove(operationId);
    }

    private synchronized Snapshot renewAndSnapshot(long now, TimeValue requestedKeepAlive) {
      if (requestedKeepAlive != null) {
        validateKeepAlive(requestedKeepAlive);
        keepAliveMillis = requestedKeepAlive.millis();
      }
      expirationTimeMillis = now + keepAliveMillis;
      return snapshotLocked();
    }

    private synchronized WaitRegistration registerCompletionWaiter(
        long now,
        TimeValue requestedKeepAlive,
        TimeValue waitTimeout,
        ActionListener<Snapshot> listener) {
      if (requestedKeepAlive != null) {
        validateKeepAlive(requestedKeepAlive);
        keepAliveMillis = requestedKeepAlive.millis();
      }
      expirationTimeMillis = now + keepAliveMillis;
      Snapshot snapshot = snapshotLocked();
      if (waitTimeout.millis() == 0 || snapshot.status() != Status.RUNNING) {
        return WaitRegistration.immediate(snapshot, listener);
      }
      Waiter waiter = new Waiter(listener);
      waiters.add(waiter);
      return WaitRegistration.waiting(waiter, waitTimeout);
    }

    private synchronized Snapshot timeout(Waiter waiter) {
      if (!waiters.remove(waiter)) {
        return null;
      }
      return snapshotLocked();
    }

    private synchronized void removeWaiter(Waiter waiter) {
      waiters.remove(waiter);
    }

    private synchronized Publication cancel(String reason, long now) {
      List<Runnable> cancellations = new ArrayList<>();
      if (status == Status.RUNNING) {
        if (parentTask != null && !parentTask.isCancelled()) {
          cancellations.add(() -> parentTask.cancel(reason));
        }
        cancellations.addAll(activeSearchCancellations.values());
        activeSearchCancellations.clear();
        status = Status.CANCELLED;
        completionTimeMillis = now;
      }
      Publication publication = publication();
      return new Publication(publication.snapshot(), publication.waiters(), cancellations);
    }

    private synchronized boolean isExpired(long now) {
      return now >= expirationTimeMillis;
    }

    private synchronized Expiration expireIfNeeded(long now) {
      if (now < expirationTimeMillis) {
        return Expiration.NOT_EXPIRED;
      }
      List<Runnable> cancellations = new ArrayList<>();
      if (status == Status.RUNNING) {
        if (parentTask != null && !parentTask.isCancelled()) {
          cancellations.add(() -> parentTask.cancel("PPL asynchronous job expired"));
        }
        cancellations.addAll(activeSearchCancellations.values());
        activeSearchCancellations.clear();
      }
      List<Waiter> expiredWaiters = new ArrayList<>(waiters);
      waiters.clear();
      return new Expiration(true, expiredWaiters, cancellations);
    }

    private synchronized Snapshot snapshot() {
      return snapshotLocked();
    }

    private Snapshot snapshotLocked() {
      long tookMillis =
          completionTimeMillis < 0 ? -1L : Math.max(0L, completionTimeMillis - startTimeMillis);
      return new Snapshot(
          id,
          status,
          classified,
          updateMode,
          progress,
          response,
          failure,
          startTimeMillis,
          expirationTimeMillis,
          tookMillis);
    }

    private Publication publication() {
      Snapshot snapshot = snapshotLocked();
      List<Waiter> ready = new ArrayList<>();
      waiters.removeIf(
          waiter -> {
            if (waiter.shouldComplete(snapshot)) {
              ready.add(waiter);
              return true;
            }
            return false;
          });
      return new Publication(snapshot, ready);
    }

    private QueryProgress runningProgress(QueryProgress candidate) {
      Objects.requireNonNull(candidate);
      double bounded = Math.min(0.8D, candidate.fractionDone());
      return new QueryProgress(Math.max(progress.fractionDone(), bounded));
    }
  }
}
