/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.junit.Test;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.GetResult;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.JobTask;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Removal;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.SnapshotSource;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Transition;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryService.Failure;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryService.Status;
import org.opensearch.tasks.CancellableTask;

public class PPLAsyncQueryJobTest {
  private static final String ID = "job-id";
  private static final long START_TIME = 1_000;
  private static final long RETAINED_TIME = 2_000;
  private static final long COMPLETION_TIME = 2_025;
  private static final long KEEP_ALIVE_MILLIS = TimeValue.timeValueMinutes(5).millis();

  @Test
  public void retainPublishesRunningResponseWithId() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));

    Transition transition = fixture.job().retain(RETAINED_TIME);

    // Retention publishes the job ID once; later retention events are ignored.
    assertEquals(
        new Transition.Retained(new SnapshotSource.Running(ID, Optional.of(fixture.execution()))),
        transition);
    assertNull(fixture.job().retain(RETAINED_TIME + 1));
  }

  @Test
  public void completionBeforeRetentionReturnsDirectResponseAndRemovesJob() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));

    Transition transition = fixture.job().complete(COMPLETION_TIME);

    // A direct response removes the job: duplicate completion is ignored and GET returns not found.
    assertTrue(transition instanceof Transition.DirectResponse);
    Transition.DirectResponse direct = (Transition.DirectResponse) transition;
    assertEquals(
        new SnapshotSource.Succeeded(
            Optional.empty(), fixture.execution(), COMPLETION_TIME - START_TIME),
        direct.response());
    assertSame(fixture.task(), direct.task());
    assertEquals(Optional.of(fixture.execution()), direct.executionToClose());
    assertNull(fixture.job().complete(COMPLETION_TIME + 1));
    assertThrows(ResourceNotFoundException.class, () -> fixture.job().get(COMPLETION_TIME, null));
  }

  @Test
  public void failureBeforeRetentionReturnsDirectResponseAndRemovesJob() {
    JobFixture fixture = newJob();
    Failure failure = new Failure("IllegalArgumentException", "invalid query");
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));

    Transition transition = fixture.job().fail(failure, COMPLETION_TIME);

    // A failure before retention returns without an ID and detaches all execution resources.
    assertTrue(transition instanceof Transition.DirectResponse);
    Transition.DirectResponse direct = (Transition.DirectResponse) transition;
    assertEquals(
        new SnapshotSource.Failed(Optional.empty(), failure, COMPLETION_TIME - START_TIME),
        direct.response());
    assertSame(fixture.task(), direct.task());
    assertEquals(Optional.of(fixture.execution()), direct.executionToClose());

    // A direct failure removes the job: later completion is ignored and GET returns not found.
    assertNull(fixture.job().complete(COMPLETION_TIME + 1));
    assertThrows(ResourceNotFoundException.class, () -> fixture.job().get(COMPLETION_TIME, null));
  }

  @Test
  public void retainedSuccessRemainsReadableUntilDeleted() {
    JobFixture fixture = retainedJob();

    Transition transition = fixture.job().complete(COMPLETION_TIME);

    // Retained success closes the task but keeps the execution readable until DELETE.
    assertEquals(new Transition.ExecutionFinished(fixture.task(), Optional.empty()), transition);

    GetResult result = fixture.job().get(COMPLETION_TIME, null);
    assertTrue(result instanceof GetResult.Found);
    assertEquals(
        new SnapshotSource.Succeeded(
            Optional.of(ID), fixture.execution(), COMPLETION_TIME - START_TIME),
        ((GetResult.Found) result).response());

    Removal removal = fixture.job().delete(COMPLETION_TIME);
    assertTrue(removal instanceof Removal.Deleted);
    Removal.Deleted deleted = (Removal.Deleted) removal;
    assertEquals(Status.SUCCEEDED, deleted.responseStatus());
    assertNull(deleted.resources().task());
    assertSame(fixture.execution(), deleted.resources().execution());
    assertEquals("PPL asynchronous query cancelled by user", deleted.reason());
  }

  @Test
  public void retainedFailureDetachesExecutionAndRemainsReadable() {
    JobFixture fixture = retainedJob();
    Failure failure = new Failure("IllegalStateException", "query failed");

    Transition transition = fixture.job().fail(failure, COMPLETION_TIME);

    // Retained failure closes its execution and keeps only the failure snapshot readable.
    assertEquals(
        new Transition.ExecutionFinished(fixture.task(), Optional.of(fixture.execution())),
        transition);

    GetResult result = fixture.job().get(COMPLETION_TIME, null);
    assertTrue(result instanceof GetResult.Found);
    assertEquals(
        new SnapshotSource.Failed(Optional.of(ID), failure, COMPLETION_TIME - START_TIME),
        ((GetResult.Found) result).response());
  }

  @Test
  public void getWithoutKeepAlivePreservesExpiration() {
    JobFixture fixture = retainedJob();
    long expirationTime = RETAINED_TIME + KEEP_ALIVE_MILLIS;

    assertTrue(fixture.job().get(expirationTime - 1, null) instanceof GetResult.Found);
    GetResult result = fixture.job().get(expirationTime, null);

    // Without renewal, the lease remains valid before but not at its expiration boundary.
    assertTrue(result instanceof GetResult.Expired);
    Removal.Expired removal = ((GetResult.Expired) result).removal();
    assertSame(fixture.task(), removal.resources().task());
    assertSame(fixture.execution(), removal.resources().execution());
    assertEquals("PPL asynchronous query expired", removal.reason());
  }

  @Test
  public void getWithKeepAliveReplacesExpiration() {
    JobFixture fixture = retainedJob();
    long renewalTime = RETAINED_TIME + 500;
    TimeValue requestedKeepAlive = TimeValue.timeValueSeconds(1);
    long renewedExpiration = renewalTime + requestedKeepAlive.millis();

    // At 2500ms, GET replaces the original lease with a one-second lease ending at 3500ms.
    GetResult renewed = fixture.job().get(renewalTime, requestedKeepAlive);
    // At 3499ms, the renewed lease has not expired.
    GetResult beforeExpiration = fixture.job().get(renewedExpiration - 1, null);
    // At 3500ms, the renewed lease expires.
    GetResult atExpiration = fixture.job().get(renewedExpiration, null);

    assertTrue(renewed instanceof GetResult.Found);
    assertTrue(beforeExpiration instanceof GetResult.Found);
    assertTrue(atExpiration instanceof GetResult.Expired);
  }

  @Test
  public void expirationStartsWhenJobIsRetained() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));
    long retainedTime = START_TIME + KEEP_ALIVE_MILLIS + 1;

    // Keep-alive starts when the ID is retained, not when the unretained job is created.
    assertNull(fixture.job().expire(retainedTime - 1));
    fixture.job().retain(retainedTime);
    assertNull(fixture.job().expire(retainedTime + KEEP_ALIVE_MILLIS - 1));
    Removal removal = fixture.job().expire(retainedTime + KEEP_ALIVE_MILLIS);
    assertTrue(removal instanceof Removal.Expired);
    Removal.Expired expired = (Removal.Expired) removal;
    assertSame(fixture.task(), expired.resources().task());
    assertSame(fixture.execution(), expired.resources().execution());
    assertEquals("PPL asynchronous query expired", expired.reason());
  }

  @Test
  public void deleteRunningJobReturnsCancellationAndDetachesResources() {
    JobFixture fixture = retainedJob();

    Removal removal = fixture.job().delete(RETAINED_TIME + 1);

    // DELETE cancels a running job, transfers its resources, and makes later DELETE return not
    // found.
    assertTrue(removal instanceof Removal.Deleted);
    Removal.Deleted deleted = (Removal.Deleted) removal;
    assertEquals(Status.CANCELLED, deleted.responseStatus());
    assertSame(fixture.task(), deleted.resources().task());
    assertSame(fixture.execution(), deleted.resources().execution());
    assertEquals("PPL asynchronous query cancelled by user", deleted.reason());
    assertTrue(deleted.resources().releasesRunningSlot());
    assertThrows(ResourceNotFoundException.class, () -> fixture.job().delete(RETAINED_TIME + 2));
  }

  @Test
  public void abortDetachesResourcesOnlyOnce() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));

    Removal removal = fixture.job().abort();

    // Abort transfers resource ownership once; repeated abort calls have no transition.
    assertTrue(removal instanceof Removal.Discarded);
    Removal.Discarded discarded = (Removal.Discarded) removal;
    assertSame(fixture.task(), discarded.resources().task());
    assertSame(fixture.execution(), discarded.resources().execution());
    assertEquals("PPL asynchronous query startup failed", discarded.reason());
    assertNull(fixture.job().abort());
  }

  @Test
  public void closeDetachesResourcesOnlyOnce() {
    JobFixture fixture = retainedJob();

    Removal removal = fixture.job().close("service closing");

    // Close transfers resource ownership once; repeated close calls have no transition.
    assertTrue(removal instanceof Removal.Discarded);
    Removal.Discarded discarded = (Removal.Discarded) removal;
    assertSame(fixture.task(), discarded.resources().task());
    assertSame(fixture.execution(), discarded.resources().execution());
    assertEquals("service closing", discarded.reason());
    assertNull(fixture.job().close("service closing"));
  }

  @Test
  public void lateExecutionAttachmentAfterRemovalIsRejected() {
    JobFixture fixture = newJob();
    fixture.job().abort();

    // Removal wins the race, so a late execution handle remains owned by the caller.
    assertFalse(fixture.job().tryAttachExecution(fixture.execution()));
  }

  @Test
  public void successfulCompletionRequiresAttachedExecution() {
    JobFixture fixture = newJob();

    // Success cannot be published until an execution handle can provide the final result.
    assertThrows(IllegalStateException.class, () -> fixture.job().complete(COMPLETION_TIME));
  }

  private static JobFixture retainedJob() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));
    fixture.job().retain(RETAINED_TIME);
    return fixture;
  }

  private static JobFixture newJob() {
    JobTask task = new JobTask(mock(CancellableTask.class), () -> {});
    AsyncQueryExecution execution = mock(AsyncQueryExecution.class);
    return new JobFixture(
        new PPLAsyncQueryJob(ID, PPLAsyncQueryUser.UNSECURED, START_TIME, KEEP_ALIVE_MILLIS, task),
        task,
        execution);
  }

  private record JobFixture(PPLAsyncQueryJob job, JobTask task, AsyncQueryExecution execution) {}
}
