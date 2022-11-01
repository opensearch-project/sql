/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.executor.streaming.Batch;
import org.opensearch.sql.executor.streaming.Offset;
import org.opensearch.sql.filesystem.storage.split.FileSystemSplit;

@ExtendWith(MockitoExtension.class)
class FileSystemStreamSourceTest {

  @TempDir
  Path perTestTempDir;

  FileSystemStreamSource streamSource;

  @BeforeEach
  void setup() {
    streamSource =
        new FileSystemStreamSource(
            FileSystems.getDefault(),
            perTestTempDir.toString());
  }

  @Test
  void getBatchFromFolder() throws IOException {
    Path file = Files.createFile(perTestTempDir.resolve("log.2022.01.01"));
    assertTrue(file.toFile().exists());

    Optional<Offset> latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isPresent());
    assertEquals(new Offset(0L), latestOffset.get());

    // fetch batch (empty, latestOffset]
    assertEquals(
        Collections.singletonList(
            new FileSystemSplit(ImmutableSet.of(file))),
        streamSource.getBatch(Optional.empty(), latestOffset.get()).getSplits());
  }

  @Test
  void latestOffsetShouldIncreaseIfNoNewFileAdded() throws IOException {
    Path file1 = Files.createFile(perTestTempDir.resolve("log.2022.01.01"));
    assertTrue(file1.toFile().exists());

    Optional<Offset> latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isPresent());
    assertEquals(new Offset(0L), latestOffset.get());

    Path file2 = Files.createFile(perTestTempDir.resolve("log.2022.01.02"));
    assertTrue(file2.toFile().exists());

    latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isPresent());
    assertEquals(new Offset(1L), latestOffset.get());

    // fetch batch (empty, 1L]
    assertBatchEquals(
        ImmutableList.of(file1, file2),
        streamSource.getBatch(Optional.empty(), latestOffset.get()));

    // fetch batch (empty, 0L]
    assertBatchEquals(
        ImmutableList.of(file1), streamSource.getBatch(Optional.empty(), new Offset(0L)));

    // fetch batch (0L, 1L]
    assertBatchEquals(
        ImmutableList.of(file2),
        streamSource.getBatch(Optional.of(new Offset(0L)), new Offset(1L)));
  }

  @Test
  void latestOffsetShouldSameIfNoNewFileAdded() throws IOException {
    Path file1 = Files.createFile(perTestTempDir.resolve("log.2022.01.01"));
    assertTrue(file1.toFile().exists());

    Optional<Offset> latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isPresent());
    assertEquals(new Offset(0L), latestOffset.get());

    // no new files.
    latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isPresent());
    assertEquals(new Offset(0L), latestOffset.get());
  }

  @Test
  void latestOffsetIsEmptyIfNoFilesInSource() {
    Optional<Offset> latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isEmpty());
  }

  @Test
  void getBatchOutOfRange() throws IOException {
    Path file = Files.createFile(perTestTempDir.resolve("log.2022.01.01"));
    assertTrue(file.toFile().exists());

    Optional<Offset> latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isPresent());
    assertEquals(new Offset(0L), latestOffset.get());

    assertEquals(
        Collections.singletonList(
            new FileSystemSplit(ImmutableSet.of(file))),
        streamSource.getBatch(Optional.empty(), latestOffset.get()).getSplits());
  }

  @Test
  void dirIsFiltered() throws IOException {
    Path file = Files.createFile(perTestTempDir.resolve("log.2022.01.01"));
    assertTrue(file.toFile().exists());

    Path dir = Files.createDirectory(perTestTempDir.resolve("logDir"));
    assertTrue(dir.toFile().isDirectory());

    Optional<Offset> latestOffset = streamSource.getLatestOffset();
    assertTrue(latestOffset.isPresent());
    assertEquals(new Offset(0L), latestOffset.get());

    // fetch batch (empty, latestOffset]
    assertEquals(
        Collections.singletonList(
            new FileSystemSplit(ImmutableSet.of(file))),
        streamSource.getBatch(Optional.empty(), latestOffset.get()).getSplits());
  }

  void assertBatchEquals(List<Path> expectedFiles, Batch batch) {
    assertEquals(1, batch.getSplits().size());
    assertThat(
        ((FileSystemSplit) batch.getSplits().get(0)).getPaths(),
        containsInAnyOrder(expectedFiles.toArray()));
  }
}
