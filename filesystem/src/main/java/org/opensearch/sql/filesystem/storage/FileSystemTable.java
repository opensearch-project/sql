/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.filesystem.storage;

import java.util.HashMap;
import java.util.Map;
import javax.xml.transform.stream.StreamSource;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/**
 * FileSystem Table is the generic {@link Table} implementation for all the file system. FileSystem
 * Table use Hadoop FileSystem to manipulate each file system.
 *
 * <p>More detail info https://hadoop.apache
 * .org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/index.html.
 */
@RequiredArgsConstructor
public class FileSystemTable implements Table {

  /**
   * Hadoop FileSystem.
   */
  private final FileSystem fileSystem;

  /**
   * Table's base Path.
   */
  private final Path basePath;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return new HashMap<>();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    throw new UnsupportedOperationException("Unsupported operation");
  }
}
