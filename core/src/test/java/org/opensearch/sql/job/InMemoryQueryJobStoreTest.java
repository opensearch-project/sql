/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class InMemoryQueryJobStoreTest {

  @Test
  void register_returnsNullOnFirstInsertion() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-1");
    assertEquals(null, store.register(job));
    assertSame(job, store.find(job.id()).orElseThrow());
  }

  @Test
  void register_returnsExistingJobOnDuplicateId() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob first = newJob("ctx-duplicate");
    QueryJob second =
        new QueryJob(first.id(), Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
    assertEquals(null, store.register(first));
    assertSame(first, store.register(second));
  }

  @Test
  void remove_isConditional() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-remove");
    store.register(job);
    QueryJob other =
        new QueryJob(job.id(), Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
    assertFalse(store.remove(job.id(), other));
    assertTrue(store.remove(job.id(), job));
    assertEquals(Optional.empty(), store.find(job.id()));
  }

  @Test
  void jobs_returnsDefensiveCopy() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-jobs");
    store.register(job);
    assertNotNull(store.jobs());
    assertEquals(1, store.jobs().size());
    store.remove(job.id(), job);
    assertEquals(0, store.jobs().size());
  }

  @Test
  void close_rejectsFurtherRegistration() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    store.close();
    QueryJob job = newJob("ctx-after-close");
    assertThrows(IllegalStateException.class, () -> store.register(job));
  }

  @Test
  void close_cancelsResidualJobs() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-shutdown");
    store.register(job);
    store.close();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
  }

  private static QueryJob newJob(String contextId) {
    QueryJobId id = new QueryJobId("node-1", contextId);
    return new QueryJob(id, Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
  }

  private static final class NoopRunner implements QueryRunner {
    @Override
    public CompletionStage<QueryResult> run() {
      return new CompletableFuture<>();
    }

    @Override
    public void cancel() {}
  }
}
