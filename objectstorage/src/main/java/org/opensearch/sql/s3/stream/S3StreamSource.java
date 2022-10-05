/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.stream;

import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.executor.stream.Batch;
import org.opensearch.sql.executor.stream.InMemoryMetadataLog;
import org.opensearch.sql.executor.stream.MetadataLog;
import org.opensearch.sql.executor.stream.Offset;
import org.opensearch.sql.executor.stream.StreamSource;
import org.opensearch.sql.s3.storage.OSS3Object;
import org.opensearch.sql.s3.storage.S3Lister;
import org.opensearch.sql.s3.storage.S3Table;

public class S3StreamSource implements StreamSource {

  private static final Logger log = LogManager.getLogger(S3StreamSource.class);

  private final MetadataLog<S3Metadata> s3MetadataLog;

  private final S3Lister s3Lister;

  private final S3Table table;

  private Set<OSS3Object> seenObjects = new HashSet<>();

  public S3StreamSource(S3Table table) {
    try {
      this.table = table;
      this.s3Lister = new S3Lister(new URI(table.getLocation()));
      this.s3MetadataLog = new InMemoryMetadataLog<>();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<Offset> getLatestOffset() {
    Set<OSS3Object> allFiles = s3Lister.listAllObjects();

    log.info("before seenObjects {}", seenObjects);
    Set<OSS3Object> unread = Sets.difference(allFiles, seenObjects);

    // todo, sort unread objects by lastModified and add size limit for each batch

    // update seenObjects
    seenObjects = allFiles;
    log.info("after seenObjects {}", seenObjects);

    // add latest batch to s3MetadataLog
    Long latestBatchId = s3MetadataLog.getLatest().map(Pair::getKey).orElse(-1L);

    if (!unread.isEmpty()) {
      latestBatchId += 1;
      s3MetadataLog.add(latestBatchId, new S3Metadata(unread, latestBatchId));
      log.info("latestBatchId {}", latestBatchId);
      log.info("unread objects {}", unread);
      return Optional.of(new Offset(latestBatchId));
    } else {
      log.info("latestBatchId {}", latestBatchId);
      log.info("no need data");
      return latestBatchId == -1 ? Optional.empty() : Optional.of(new Offset(latestBatchId));
    }
  }

  @Override
  public Batch getBatch(Optional<Offset> start, Offset end) {
    Long startOffset = start.map(Offset::getOffset).orElse(-1L);
    Long endOffset = end.getOffset();

    final Set<OSS3Object> objects =
        s3MetadataLog.get(Optional.of(startOffset + 1), Optional.of(endOffset)).stream()
            .map(S3Metadata::getPaths).flatMap(Set::stream).collect(
            Collectors.toSet());

    log.info("return batch start: {} end: {} of objects {}", start, end, objects);
    return new S3Batch(objects, table);
  }

  @Override
  public void commit(Offset offset) {
    // do nothing.
  }

  @Override
  public String toString() {
    return "S3StreamSource{" + ", table=" + table + '}';
  }
}
