/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.executor.streaming.Offset;
import org.opensearch.sql.filesystem.storage.split.FileSystemSplit;
import org.opensearch.sql.storage.split.Split;

@ExtendWith(MockitoExtension.class)
class FileSystemStreamSourceTest {

  @TempDir Path perTestTempDir;

  FileSystemStreamSource streamSource;

  /**
   * use hadoop default filesystem. it only works on unix-like system. for running on windows, it
   * require native library. Reference.
   * https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-common/NativeLibraries.html
   */
  @BeforeEach
  void setup() throws IOException {
    streamSource =
        new FileSystemStreamSource(
            FileSystem.get(new Configuration()),
            new org.apache.hadoop.fs.Path(perTestTempDir.toUri()));
  }

  @Test
  void addOneFileToSource() throws IOException {
    emptySource().addFile("log1").latestOffsetShouldBe(0L).batchFromStart("log1");
  }

  @Test
  void addMultipleFileInSequence() throws IOException {
    emptySource()
        .addFile("log1")
        .latestOffsetShouldBe(0L)
        .batchFromStart("log1")
        .addFile("log2")
        .latestOffsetShouldBe(1L)
        .batchFromStart("log1", "log2")
        .batchInBetween(0L, 1L, "log2");
  }

  @Test
  void latestOffsetShouldSameIfNoNewFileAdded() throws IOException {
    emptySource()
        .addFile("log1")
        .latestOffsetShouldBe(0L)
        .batchFromStart("log1")
        .latestOffsetShouldBe(0L)
        .batchFromStart("log1");
  }

  @Test
  void latestOffsetIsEmptyIfNoFilesInSource() {
    emptySource().noOffset();
  }

  @Test
  void dirIsFiltered() throws IOException {
    emptySource()
        .addFile("log1")
        .latestOffsetShouldBe(0L)
        .addDir("dir1")
        .latestOffsetShouldBe(0L)
        .batchFromStart("log1");
  }

  @Test
  void sneakThrowException() throws IOException {
    FileSystem fs = Mockito.mock(FileSystem.class);
    doThrow(IOException.class).when(fs).listStatus(any(org.apache.hadoop.fs.Path.class));

    streamSource =
        new FileSystemStreamSource(fs,
            new org.apache.hadoop.fs.Path(perTestTempDir.toUri()));
    assertThrows(IOException.class, () -> streamSource.getLatestOffset());
  }

  StreamSource emptySource() {
    return new StreamSource();
  }

  private class StreamSource {

    StreamSource addFile(String filename) throws IOException {
      Path file = Files.createFile(perTestTempDir.resolve(filename));
      assertTrue(file.toFile().exists());

      return this;
    }

    StreamSource addDir(String dirname) throws IOException {
      Path dir = Files.createDirectory(perTestTempDir.resolve(dirname));
      assertTrue(dir.toFile().isDirectory());

      return this;
    }

    StreamSource noOffset() {
      assertFalse(streamSource.getLatestOffset().isPresent());

      return this;
    }

    StreamSource latestOffsetShouldBe(Long offset) {
      Optional<Offset> latestOffset = streamSource.getLatestOffset();
      assertTrue(latestOffset.isPresent());
      assertEquals(new Offset(offset), latestOffset.get());

      return this;
    }

    StreamSource batchFromStart(String... uris) {
      assertTrue(streamSource.getLatestOffset().isPresent());
      internalBatchInBetween(Optional.empty(), streamSource.getLatestOffset().get(), uris);

      return this;
    }

    StreamSource batchInBetween(Long start, Long end, String... uris) {
      internalBatchInBetween(Optional.of(new Offset(start)), new Offset(end), uris);

      return this;
    }

    private StreamSource internalBatchInBetween(
        Optional<Offset> start, Offset end, String... uris) {
      Split split = streamSource.getBatch(start, end).getSplit();
      assertThat(
          ((FileSystemSplit) split).getPaths(),
          containsInAnyOrder(
              Arrays.stream(uris)
                  .map(name -> new org.apache.hadoop.fs.Path(perTestTempDir.resolve(name).toUri()))
                  .toArray()));
      return this;
    }
  }
}
