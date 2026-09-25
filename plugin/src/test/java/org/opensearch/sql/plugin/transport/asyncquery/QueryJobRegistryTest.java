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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.mockito.Mockito;

public class QueryJobRegistryTest {

  @Test
  public void addPublishesJobAndReturnsExistingOnCollision() {
    QueryJobRegistry registry = new QueryJobRegistry();
    QueryJobId sharedId = new QueryJobId("node-a", "context-1");
    QueryJob first = jobWithId(sharedId);
    QueryJob second = jobWithId(sharedId);

    assertNull(registry.add(first));
    assertSame(first, registry.add(second));
    assertSame(first, registry.get(sharedId).orElseThrow());
  }

  @Test
  public void removeOnlyDropsGivenJobInstance() {
    QueryJobRegistry registry = new QueryJobRegistry();
    QueryJobId sharedId = new QueryJobId("node-a", "context-1");
    QueryJob first = jobWithId(sharedId);
    QueryJob replacement = jobWithId(sharedId);

    registry.add(first);
    assertFalse(registry.remove(sharedId, replacement));
    assertSame(first, registry.get(sharedId).orElseThrow());
    assertTrue(registry.remove(sharedId, first));
    assertEquals(Optional.empty(), registry.get(sharedId));
  }

  @Test
  public void closeDiscardsEveryJobAndRejectsFurtherAdds() {
    QueryJobRegistry registry = new QueryJobRegistry();
    QueryJob first = jobWithId(newId());
    QueryJob second = jobWithId(newId());
    registry.add(first);
    registry.add(second);

    registry.close();

    Mockito.verify(first).discard("PPL asynchronous query service is closing");
    Mockito.verify(second).discard("PPL asynchronous query service is closing");
    assertThrows(IllegalStateException.class, () -> registry.add(jobWithId(newId())));
  }

  @Test
  public void closeIsIdempotent() {
    QueryJobRegistry registry = new QueryJobRegistry();
    QueryJob job = jobWithId(newId());
    registry.add(job);

    registry.close();
    registry.close();

    Mockito.verify(job, Mockito.times(1)).discard(Mockito.anyString());
  }

  @Test
  public void jobsReturnsDefensiveSnapshot() {
    QueryJobRegistry registry = new QueryJobRegistry();
    QueryJob job = jobWithId(newId());
    registry.add(job);

    var snapshot = registry.jobs();
    registry.remove(job.getJobId(), job);

    assertTrue(snapshot.contains(job));
    assertEquals(0, registry.jobs().size());
  }

  private static final AtomicInteger COUNTER = new AtomicInteger();

  private static QueryJobId newId() {
    return new QueryJobId("node-a", "context-" + COUNTER.incrementAndGet());
  }

  private static QueryJob jobWithId(QueryJobId id) {
    QueryJob job = Mockito.mock(QueryJob.class);
    Mockito.when(job.getJobId()).thenReturn(id);
    return job;
  }
}
