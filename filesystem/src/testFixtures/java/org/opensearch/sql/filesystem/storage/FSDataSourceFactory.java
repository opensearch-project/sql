/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;

@RequiredArgsConstructor
public class FSDataSourceFactory implements DataSourceFactory {

  private final URI basePath;

  private final AtomicInteger result;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.FILESYSTEM;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(), DataSourceType.FILESYSTEM, new FSStorageEngine(basePath, result));
  }
}
