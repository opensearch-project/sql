/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultMetadataLogTest {

  private DefaultMetadataLog<Long> metadataLog;

  @BeforeEach
  void setup() {
    metadataLog = new DefaultMetadataLog<>();
  }

  @Test
  void addMetadataShouldSuccess() {
    assertTrue(metadataLog.add(0L, 0L));
    assertTrue(metadataLog.add(1L, 1L));
  }

  @Test
  void addMetadataWithSameBatchIdShouldFail() {
    assertTrue(metadataLog.add(0L, 0L));
    assertFalse(metadataLog.add(0L, 1L));
  }

  @Test
  void addMetadataWithInvalidIdShouldThrowException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> metadataLog.add(-1L, 0L));
    assertEquals("batch id must large or equal 0", exception.getMessage());
  }

  @Test
  void getWithIdReturnMetadata() {
    metadataLog.add(0L, 0L);

    assertTrue(metadataLog.get(0L).isPresent());
    assertEquals(0L, metadataLog.get(0L).get());
  }

  @Test
  void getWithNotExistShouldReturnEmtpy() {
    metadataLog.add(0L, 0L);

    assertTrue(metadataLog.get(1L).isEmpty());
    assertTrue(metadataLog.get(-1L).isEmpty());
  }

  @Test
  void getWithIdInRangeShouldReturnMetadataList() {
    metadataLog.add(0L, 0L);
    metadataLog.add(1L, 1L);
    metadataLog.add(2L, 2L);

    assertEquals(Arrays.asList(0L, 1L, 2L), metadataLog.get(Optional.of(0L), Optional.of(2L)));
    assertEquals(Arrays.asList(0L, 1L, 2L), metadataLog.get(Optional.of(0L), Optional.of(4L)));
    assertEquals(Arrays.asList(0L, 1L, 2L), metadataLog.get(Optional.of(-1L), Optional.of(4L)));
    assertEquals(Arrays.asList(0L, 1L), metadataLog.get(Optional.of(0L), Optional.of(1L)));
    assertEquals(Arrays.asList(1L, 2L), metadataLog.get(Optional.of(1L), Optional.empty()));
    assertEquals(Arrays.asList(0L, 1L), metadataLog.get(Optional.empty(), Optional.of(1L)));
    assertEquals(Arrays.asList(0L, 1L, 2L), metadataLog.get(Optional.empty(), Optional.empty()));
  }

  @Test
  void getWithIdOutOfRangeShouldReturnEmpty() {
    metadataLog.add(0L, 0L);
    metadataLog.add(1L, 1L);
    metadataLog.add(2L, 2L);

    assertTrue(metadataLog.get(Optional.of(3L), Optional.of(5L)).isEmpty());
  }

  @Test
  void getLatestShouldReturnMetadata() {
    metadataLog.add(0L, 10L);
    metadataLog.add(1L, 11L);

    Optional<Pair<Long, Long>> latest = metadataLog.getLatest();
    assertTrue(latest.isPresent());
    assertEquals(1L, latest.get().getLeft());
    assertEquals(11L, latest.get().getRight());
  }

  @Test
  void getLatestFromEmptyWALShouldReturnEmpty() {
    Optional<Pair<Long, Long>> latest = metadataLog.getLatest();
    assertTrue(latest.isEmpty());
  }

  @Test
  void purgeLatestShouldOnlyKeepLatest() {
    metadataLog.add(0L, 10L);
    metadataLog.add(1L, 11L);
    metadataLog.add(2L, 12L);

    Optional<Pair<Long, Long>> latest = metadataLog.getLatest();
    assertTrue(latest.isPresent());
    metadataLog.purge(latest.get().getLeft());

    latest = metadataLog.getLatest();
    assertTrue(latest.isPresent());
    assertEquals(2L, latest.get().getLeft());
    assertEquals(12L, latest.get().getRight());
  }
}
