/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.UpdateMode;
import org.opensearch.tasks.CancellableTask;

public class PPLAsyncQueryJobServiceTest {
  private static final String OWNER = "node-a";
  private static final User USER = new User("alice", List.of("analytics"), List.of(), List.of());

  private final AtomicLong now = new AtomicLong(1_000);
  private final PPLAsyncQueryJobService service =
      new PPLAsyncQueryJobService(OWNER, now::get, (delay, task) -> () -> {});

  @Test
  public void append_job_publishes_progress_partial_rows_and_final_rows() {
    String id = createJob();

    PPLAsyncQueryJobService.Snapshot created = service.get(id, USER, null);
    assertEquals(PPLAsyncQueryJobService.Status.RUNNING, created.status());
    assertFalse(created.classified());
    assertNull(created.response());

    service.classify(id, UpdateMode.APPEND);
    service.progress(id, new QueryProgress(0.5D));
    service.partial(id, response(1));

    PPLAsyncQueryJobService.Snapshot partial = service.get(id, USER, null);
    assertTrue(partial.classified());
    assertEquals(UpdateMode.APPEND, partial.updateMode());
    assertEquals(1, partial.response().getResults().size());
    assertEquals(0.5D, partial.progress().fractionDone(), 0D);

    now.addAndGet(25);
    service.complete(id, response(2));
    PPLAsyncQueryJobService.Snapshot completed = service.get(id, USER, null);
    assertEquals(PPLAsyncQueryJobService.Status.SUCCEEDED, completed.status());
    assertEquals(2, completed.response().getResults().size());
    assertEquals(1D, completed.progress().fractionDone(), 0D);
    assertEquals(25, completed.tookMillis());
  }

  @Test
  public void replace_job_replaces_running_snapshot_and_accepts_final_rows() {
    String id = createJob();
    service.classify(id, UpdateMode.REPLACE);
    service.progress(id, new QueryProgress(0.25D));

    service.partial(id, response(2));
    service.partial(id, response(1));

    PPLAsyncQueryJobService.Snapshot running = service.get(id, USER, null);
    assertEquals(PPLAsyncQueryJobService.Status.RUNNING, running.status());
    assertEquals(1, running.response().getResults().size());
    assertEquals(0.25D, running.progress().fractionDone(), 0D);

    service.complete(id, response(2));
    assertEquals(2, service.get(id, USER, null).response().getResults().size());
  }

  @Test
  public void partial_rows_and_progress_are_committed_together() {
    String id = createJob();
    service.classify(id, UpdateMode.APPEND);

    service.partial(id, response(1), new QueryProgress(0.25D));

    PPLAsyncQueryJobService.Snapshot snapshot = service.get(id, USER, null);
    assertEquals(1, snapshot.response().getResults().size());
    assertEquals(new QueryProgress(0.25D), snapshot.progress());
  }

  @Test
  public void failed_job_preserves_failure() {
    String id = createJob();
    IllegalStateException failure = new IllegalStateException("boom");

    service.fail(id, failure);

    PPLAsyncQueryJobService.Snapshot snapshot = service.get(id, USER, null);
    assertEquals(PPLAsyncQueryJobService.Status.FAILED, snapshot.status());
    assertEquals(failure, snapshot.failure());
  }

  @Test
  public void rejects_disjoint_backend_roles() {
    String id = createJob();
    User disjoint = new User("bob", List.of("finance"), List.of(), List.of());
    assertThrows(OpenSearchSecurityException.class, () -> service.get(id, disjoint, null));
  }

  @Test
  public void permits_a_different_user_with_a_backend_role_superset() {
    String id = createJob();
    User superset = new User("bob", List.of("analytics", "administrators"), List.of(), List.of());

    assertEquals(id, service.get(id, superset, null).id());
  }

  @Test
  public void an_empty_submitter_role_set_is_visible_to_any_authorized_caller() {
    String id =
        service.create(
            new User("alice", List.of(), List.of(), List.of()),
            PPLAsyncQueryJobService.DEFAULT_KEEP_ALIVE,
            null);
    User caller = new User("bob", List.of("finance"), List.of(), List.of());

    assertEquals(id, service.get(id, caller, null).id());
  }

  @Test
  public void expires_and_cancels_running_job() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    String id = service.create(USER, TimeValue.timeValueSeconds(1), task);

    now.addAndGet(1_001);

    assertThrows(ResourceNotFoundException.class, () -> service.get(id, USER, null));
    verify(task).cancel("PPL asynchronous job expired");
  }

  @Test
  public void delete_cancels_releases_and_subsequent_get_is_not_found() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    String id = service.create(USER, TimeValue.timeValueMinutes(1), task);

    PPLAsyncQueryJobService.Snapshot snapshot = service.cancelAndRemove(id, USER);

    assertEquals(PPLAsyncQueryJobService.Status.CANCELLED, snapshot.status());
    verify(task).cancel("PPL asynchronous job cancelled by user");
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, USER, null));
  }

  @Test
  public void get_renews_the_lease() {
    String id = createJob();
    long originalExpiration = service.get(id, USER, null).expirationTimeMillis();
    now.addAndGet(500);

    long renewedExpiration = service.get(id, USER, null).expirationTimeMillis();

    assertTrue(renewedExpiration > originalExpiration);
  }

  @Test
  public void submit_wait_completes_when_job_finishes() {
    AtomicReference<Runnable> timeoutTask = new AtomicReference<>();
    PPLAsyncQueryJobService longPollService =
        new PPLAsyncQueryJobService(
            OWNER,
            now::get,
            (delay, task) -> {
              timeoutTask.set(task);
              return () -> {};
            });
    String id = longPollService.create(USER, PPLAsyncQueryJobService.DEFAULT_KEEP_ALIVE, null);
    AtomicReference<PPLAsyncQueryJobService.Snapshot> result = new AtomicReference<>();

    longPollService.awaitCompletion(
        id,
        USER,
        TimeValue.timeValueSeconds(1),
        ActionListener.wrap(
            result::set,
            e -> {
              throw new AssertionError(e);
            }));
    assertNull(result.get());
    assertTrue(timeoutTask.get() != null);

    longPollService.complete(id, response(1));

    assertEquals(PPLAsyncQueryJobService.Status.SUCCEEDED, result.get().status());
  }

  @Test
  public void submit_wait_timeout_returns_the_current_snapshot() {
    AtomicReference<Runnable> timeoutTask = new AtomicReference<>();
    PPLAsyncQueryJobService longPollService =
        new PPLAsyncQueryJobService(
            OWNER,
            now::get,
            (delay, task) -> {
              timeoutTask.set(task);
              return () -> {};
            });
    String id = longPollService.create(USER, PPLAsyncQueryJobService.DEFAULT_KEEP_ALIVE, null);
    AtomicReference<PPLAsyncQueryJobService.Snapshot> result = new AtomicReference<>();

    longPollService.awaitCompletion(
        id,
        USER,
        TimeValue.timeValueSeconds(1),
        ActionListener.wrap(
            result::set,
            e -> {
              throw new AssertionError(e);
            }));
    timeoutTask.get().run();

    assertEquals(PPLAsyncQueryJobService.Status.RUNNING, result.get().status());
  }

  @Test
  public void running_progress_is_monotonic_and_capped_at_eighty_percent() {
    String id = createJob();

    service.progress(id, new QueryProgress(0.6D));
    service.progress(id, new QueryProgress(0.4D));
    service.progress(id, new QueryProgress(0.95D));

    assertEquals(0.8D, service.get(id, USER, null).progress().fractionDone(), 0D);
  }

  @Test
  public void jobIdentifierRoundTripsOwnerAndContext() {
    PPLAsyncQueryJobId original = PPLAsyncQueryJobId.create(OWNER);

    assertEquals(original, PPLAsyncQueryJobId.parse(original.encode()));
    assertThrows(IllegalArgumentException.class, () -> PPLAsyncQueryJobId.parse("invalid"));
    assertThrows(IllegalArgumentException.class, () -> PPLAsyncQueryJobId.parse(""));
    assertThrows(IllegalArgumentException.class, () -> new PPLAsyncQueryJobId("", "context"));
  }

  @Test
  public void owner_node_id_is_resolved_lazily() {
    AtomicReference<String> owner = new AtomicReference<>();
    PPLAsyncQueryJobService lazyService =
        new PPLAsyncQueryJobService(owner::get, mock(org.opensearch.threadpool.ThreadPool.class));

    owner.set(OWNER);
    String id = lazyService.create(USER, PPLAsyncQueryJobService.DEFAULT_KEEP_ALIVE, null);

    assertEquals(OWNER, PPLAsyncQueryJobId.parse(id).ownerNodeId());
  }

  private String createJob() {
    return service.create(USER, PPLAsyncQueryJobService.DEFAULT_KEEP_ALIVE, null);
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
}
