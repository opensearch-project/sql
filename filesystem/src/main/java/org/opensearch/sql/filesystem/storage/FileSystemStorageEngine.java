/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.filesystem.storage;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opensearch.sql.catalog.model.Catalog;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * FileSystem StorageEngine is the generic {@link StorageEngine} implementation for all the file
 * system. FileSystem StorageEngine is configured through {@link Catalog} configuration.
 *
 * <p>More detail info https://hadoop.apache
 * .org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/index.html.
 *
 * <ul>
 *   <li> AWS S3 configuration, follow https://hadoop.apache
 *   .org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
 * </ul>
 */
@RequiredArgsConstructor
public class FileSystemStorageEngine implements StorageEngine {

  /**
   * Hadoop FileSystem Configuration.
   */
  private final Configuration conf;

  @SneakyThrows
  @Override
  public Table getTable(String name) {
    return new FileSystemTable(FileSystem.get(conf), new Path(name));
  }
}
