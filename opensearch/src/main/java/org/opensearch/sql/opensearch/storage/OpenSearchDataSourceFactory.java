/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.storage.DataSourceFactory;

@RequiredArgsConstructor
public class OpenSearchDataSourceFactory implements DataSourceFactory {

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  private final Settings settings;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.OPENSEARCH;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.OPENSEARCH,
        new OpenSearchStorageEngine(client, settings));
  }
}
