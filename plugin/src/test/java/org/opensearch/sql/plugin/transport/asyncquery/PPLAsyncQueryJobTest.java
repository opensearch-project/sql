/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.GetResult;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.JobTask;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Removal;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.ResponseContext;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Retention;
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

    assertEquals(
        new Transition(
            new ResponseContext(ID, Status.RUNNING, fixture.execution(), null, -1),
            Retention.RETAIN,
            null,
            null),
        transition);
    assertNull(fixture.job().retain(RETAINED_TIME + 1));
  }

  @Test
  public void completionBeforeRetentionReturnsDirectResponseAndRemovesJob() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));

    Transition transition = fixture.job().complete(COMPLETION_TIME);

    assertEquals(
        new Transition(
            new ResponseContext(
                null, Status.SUCCEEDED, fixture.execution(), null, COMPLETION_TIME - START_TIME),
            Retention.REMOVE,
            fixture.execution(),
            fixture.task()),
        transition);
    assertTrue(transition.releasesRunningSlot());
    assertNull(fixture.job().complete(COMPLETION_TIME + 1));
    assertThrows(ResourceNotFoundException.class, () -> fixture.job().get(COMPLETION_TIME, null));
  }

  @Test
  public void failureBeforeRetentionReturnsDirectResponseAndRemovesJob() {
    JobFixture fixture = newJob();
    Failure failure = new Failure("IllegalArgumentException", "invalid query");
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));

    Transition transition = fixture.job().fail(failure, COMPLETION_TIME);

    assertEquals(
        new Transition(
            new ResponseContext(
                null, Status.FAILED, fixture.execution(), failure, COMPLETION_TIME - START_TIME),
            Retention.REMOVE,
            fixture.execution(),
            fixture.task()),
        transition);
    assertTrue(transition.releasesRunningSlot());
  }

  @Test
  public void retainedSuccessRemainsReadableUntilDeleted() {
    JobFixture fixture = retainedJob();

    Transition transition = fixture.job().complete(COMPLETION_TIME);

    assertEquals(new Transition(null, Retention.RETAIN, null, fixture.task()), transition);
    assertEquals(
        new GetResult.Found(
            new ResponseContext(
                ID, Status.SUCCEEDED, fixture.execution(), null, COMPLETION_TIME - START_TIME)),
        fixture.job().get(COMPLETION_TIME, null));
    assertEquals(
        new Removal(
            Status.SUCCEEDED,
            null,
            fixture.execution(),
            "PPL asynchronous query cancelled by user",
            false),
        fixture.job().delete(COMPLETION_TIME));
  }

  @Test
  public void retainedFailureDetachesExecutionAndRemainsReadable() {
    JobFixture fixture = retainedJob();
    Failure failure = new Failure("IllegalStateException", "query failed");

    Transition transition = fixture.job().fail(failure, COMPLETION_TIME);

    assertEquals(
        new Transition(null, Retention.RETAIN, fixture.execution(), fixture.task()), transition);
    assertEquals(
        new GetResult.Found(
            new ResponseContext(ID, Status.FAILED, null, failure, COMPLETION_TIME - START_TIME)),
        fixture.job().get(COMPLETION_TIME, null));
  }

  @Test
  public void getWithoutKeepAlivePreservesExpiration() {
    JobFixture fixture = retainedJob();
    long expirationTime = RETAINED_TIME + KEEP_ALIVE_MILLIS;

    assertTrue(fixture.job().get(expirationTime - 1, null) instanceof GetResult.Found);
    GetResult result = fixture.job().get(expirationTime, null);

    assertEquals(
        new GetResult.Expired(
            new Removal(
                Status.RUNNING,
                fixture.task(),
                fixture.execution(),
                "PPL asynchronous query expired",
                true)),
        result);
  }

  @Test
  public void getWithKeepAliveReplacesExpiration() {
    JobFixture fixture = retainedJob();
    long renewalTime = RETAINED_TIME + 500;
    TimeValue requestedKeepAlive = TimeValue.timeValueSeconds(1);

    assertTrue(fixture.job().get(renewalTime, requestedKeepAlive) instanceof GetResult.Found);
    assertTrue(fixture.job().get(renewalTime + 999, null) instanceof GetResult.Found);
    assertTrue(
        fixture.job().get(renewalTime + requestedKeepAlive.millis(), null)
            instanceof GetResult.Expired);
  }

  @Test
  public void expirationStartsWhenJobIsRetained() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));
    long retainedTime = START_TIME + KEEP_ALIVE_MILLIS + 1;

    assertNull(fixture.job().expire(retainedTime - 1));
    fixture.job().retain(retainedTime);
    assertNull(fixture.job().expire(retainedTime + KEEP_ALIVE_MILLIS - 1));
    assertEquals(
        new Removal(
            Status.RUNNING,
            fixture.task(),
            fixture.execution(),
            "PPL asynchronous query expired",
            true),
        fixture.job().expire(retainedTime + KEEP_ALIVE_MILLIS));
  }

  @Test
  public void deleteRunningJobReturnsCancellationAndDetachesResources() {
    JobFixture fixture = retainedJob();

    Removal removal = fixture.job().delete(RETAINED_TIME + 1);

    assertEquals(
        new Removal(
            Status.CANCELLED,
            fixture.task(),
            fixture.execution(),
            "PPL asynchronous query cancelled by user",
            false),
        removal);
    assertTrue(removal.releasesRunningSlot());
    assertThrows(ResourceNotFoundException.class, () -> fixture.job().delete(RETAINED_TIME + 2));
  }

  @Test
  public void abortDetachesResourcesOnlyOnce() {
    JobFixture fixture = newJob();
    assertTrue(fixture.job().tryAttachExecution(fixture.execution()));

    assertEquals(
        new Removal(
            Status.RUNNING,
            fixture.task(),
            fixture.execution(),
            "PPL asynchronous query startup failed",
            false),
        fixture.job().abort());
    assertNull(fixture.job().abort());
  }

  @Test
  public void closeDetachesResourcesOnlyOnce() {
    JobFixture fixture = retainedJob();

    assertEquals(
        new Removal(Status.RUNNING, fixture.task(), fixture.execution(), "service closing", false),
        fixture.job().close("service closing"));
    assertNull(fixture.job().close("service closing"));
  }

  @Test
  public void lateExecutionAttachmentAfterRemovalIsRejected() {
    JobFixture fixture = newJob();
    fixture.job().abort();

    assertFalse(fixture.job().tryAttachExecution(fixture.execution()));
  }

  @Test
  public void successfulCompletionRequiresAttachedExecution() {
    JobFixture fixture = newJob();

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
