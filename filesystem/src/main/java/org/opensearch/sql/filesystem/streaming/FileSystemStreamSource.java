/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.streaming;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.executor.streaming.Batch;
import org.opensearch.sql.executor.streaming.DefaultMetadataLog;
import org.opensearch.sql.executor.streaming.MetadataLog;
import org.opensearch.sql.executor.streaming.Offset;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.filesystem.storage.split.FileSystemSplit;

/**
 * FileSystem Streaming Source use Hadoop FileSystem.
 */
public class FileSystemStreamSource implements StreamingSource {

  private static final Logger log = LogManager.getLogger(FileSystemStreamSource.class);

  private final MetadataLog<FileMetaData> fileMetaDataLog;

  private Set<Path> seenFiles;

  private final FileSystem fs;

  private final Path basePath;

  /**
   * Constructor of FileSystemStreamSource.
   */
  public FileSystemStreamSource(FileSystem fs, Path basePath) {
    this.fs = fs;
    this.basePath = basePath;
    // todo, need to add state recovery
    this.fileMetaDataLog = new DefaultMetadataLog<>();
    // todo, need to add state recovery
    this.seenFiles = new HashSet<>();
  }

  @SneakyThrows(value = IOException.class)
  @Override
  public Optional<Offset> getLatestOffset() {
    // list all files. todo. improvement list performance.
    Set<Path> allFiles =
        Arrays.stream(fs.listStatus(basePath))
            .filter(status -> !status.isDirectory())
            .map(FileStatus::getPath)
            .collect(Collectors.toSet());

    // find unread files.
    log.debug("all files {}", allFiles);
    Set<Path> unread = Sets.difference(allFiles, seenFiles);

    // update seenFiles.
    seenFiles = allFiles;
    log.debug("seen files {}", seenFiles);

    Optional<Long> latestBatchIdOptional = fileMetaDataLog.getLatest().map(Pair::getKey);
    if (!unread.isEmpty()) {
      long latestBatchId = latestBatchIdOptional.map(id -> id + 1).orElse(0L);
      fileMetaDataLog.add(latestBatchId, new FileMetaData(latestBatchId, unread));
      log.debug("latestBatchId {}", latestBatchId);
      return Optional.of(new Offset(latestBatchId));
    } else {
      log.debug("no unread data");
      Optional<Offset> offset =
          latestBatchIdOptional.isEmpty()
              ? Optional.empty()
              : Optional.of(new Offset(latestBatchIdOptional.get()));
      log.debug("return empty offset {}", offset);
      return offset;
    }
  }

  @Override
  public Batch getBatch(Optional<Offset> start, Offset end) {
    Long startBatchId = start.map(Offset::getOffset).map(id -> id + 1).orElse(0L);
    Long endBatchId = end.getOffset();

    Set<Path> paths =
        fileMetaDataLog.get(Optional.of(startBatchId), Optional.of(endBatchId)).stream()
            .map(FileMetaData::getPaths)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

    log.debug("fetch files {} with id from: {} to: {}.", paths, start, end);
    return new Batch(Collections.singletonList(new FileSystemSplit(paths)));
  }
}
