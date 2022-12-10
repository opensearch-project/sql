/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/** FileSystem StorageEngine. Used for testing purpose. */
@RequiredArgsConstructor
public class FSStorageEngine implements StorageEngine {

  private final FileSystem fs;

  private final Path basePath;

  private final AtomicInteger result;

  /**
   * constructor.
   */
  @SneakyThrows
  public FSStorageEngine(URI basePath, AtomicInteger result) {
    this.fs = FileSystem.get(new Configuration());
    this.basePath = new Path(basePath);
    this.result = result;
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
    return new FSTable(fs, basePath, result);
  }

  @SneakyThrows
  public void close() {
    fs.close();
  }
}
