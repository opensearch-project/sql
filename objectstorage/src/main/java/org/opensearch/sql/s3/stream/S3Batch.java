/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.stream;

import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.executor.stream.Batch;
import org.opensearch.sql.s3.storage.OSS3Object;
import org.opensearch.sql.s3.storage.S3Table;
import org.opensearch.sql.storage.Table;

@RequiredArgsConstructor
public class S3Batch implements Batch {

  private final Set<OSS3Object> files;

  private final S3Table table;

  @Override
  public Table toTable() {
    return new S3StreamTable(files, table);
  }
}
